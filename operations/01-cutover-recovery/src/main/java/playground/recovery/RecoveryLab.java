package playground.recovery;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.springframework.core.SpringVersion;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import javax.sql.DataSource;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

public class RecoveryLab {
    final JdbcTemplate db;
    final TransactionTemplate tx;
    RecoveryLab(DataSource ds) { db=new JdbcTemplate(ds);tx=new TransactionTemplate(new DataSourceTransactionManager(ds)); }
    static void check(boolean ok,String why) { if(!ok)throw new IllegalStateException(why); }
    // Schema names are program-owned constants, never user inputs.
    void legacy(String schema) {
        check(Set.of("legacy","rollback_candidate","blocked_candidate").contains(schema),"Unknown schema");
        db.execute("CREATE SCHEMA "+schema);
        db.execute("CREATE TABLE "+schema+".posts(id bigserial PRIMARY KEY,title text NOT NULL,body text NOT NULL,cover text NOT NULL)");
        db.execute("CREATE TABLE "+schema+".tags(post_id bigint REFERENCES "+schema+".posts(id),tag text,PRIMARY KEY(post_id,tag))");
    }
    List<String> oldRows(String schema) {
        check(Set.of("legacy","rollback_candidate","blocked_candidate").contains(schema),"Unknown schema");
        return db.queryForList("SELECT jsonb_build_object('id',p.id,'title',p.title,'body',p.body,'cover',p.cover,'tags',"
            +"COALESCE((SELECT jsonb_agg(tag ORDER BY tag) FROM "+schema+".tags WHERE post_id=p.id),'[]'::jsonb))::text "
            +"FROM "+schema+".posts p ORDER BY p.id",String.class);
    }
    List<String> newRows(String schema) {
        check(Set.of("modern","latest").contains(schema),"Unknown schema");
        return db.queryForList("SELECT jsonb_build_object('id',id,'title',title,'body',content,'cover',cover,'tags',"
            +"(SELECT jsonb_agg(t ORDER BY t) FROM jsonb_array_elements_text(tags) t))::text FROM "+schema+".posts ORDER BY id",String.class);
    }
    void reverse(String target) {
        check(Set.of("rollback_candidate","blocked_candidate").contains(target),"Unknown target");
        tx.executeWithoutResult(status->{
            check(db.queryForObject("SELECT count(*) FROM "+target+".posts",Long.class)==0,"Target not empty");
            check(db.queryForObject("SELECT count(*) FROM latest.posts WHERE format<>'markdown'",Long.class)==0,"Unsupported old format");
            db.update("INSERT INTO "+target+".posts(id,title,body,cover) SELECT id,title,content,cover FROM latest.posts");
            db.update("INSERT INTO "+target+".tags SELECT p.id,t.tag FROM latest.posts p CROSS JOIN LATERAL jsonb_array_elements_text(p.tags) t(tag)");
            check(oldRows(target).equals(newRows("latest")),"Reverse values differ");
        });
    }
    static String hash(Path path) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path))); }
    static List<String> invalidObjects(Path directory,Map<String,String> manifest) throws Exception {
        var bad=new ArrayList<String>();
        for(var e:manifest.entrySet()) {
            var path=directory.resolve(e.getKey());
            if(!Files.isRegularFile(path)||!hash(path).equals(e.getValue()))bad.add(e.getKey());
        }
        Collections.sort(bad);return bad;
    }
    void run(Path root) throws Exception {
        Path live=Files.createDirectory(root.resolve("live"));Path oldObjects=Files.createDirectory(root.resolve("before"));Path restored=Files.createDirectory(root.resolve("restored"));
        for(int i=1;i<=2;i++){String name="cover-"+i+".bin";Files.writeString(live.resolve(name),"fixture-bytes-"+i);Files.copy(live.resolve(name),oldObjects.resolve(name));}
        legacy("legacy");legacy("rollback_candidate");legacy("blocked_candidate");
        db.update("INSERT INTO legacy.posts(title,body,cover) VALUES ('before-one','body-one','cover-1.bin'),('before-two','body-two','cover-2.bin')");
        db.update("INSERT INTO legacy.tags VALUES (1,'spring'),(1,'db'),(2,'old')");var before=oldRows("legacy");
        db.execute("CREATE SCHEMA modern");
        db.execute("CREATE TABLE modern.posts(id bigserial PRIMARY KEY,title text NOT NULL,content text NOT NULL,cover text NOT NULL,tags jsonb NOT NULL,format text NOT NULL)");
        tx.executeWithoutResult(status->{
            db.update("INSERT INTO modern.posts SELECT p.id,p.title,p.body,p.cover,(SELECT jsonb_agg(tag ORDER BY tag) FROM legacy.tags WHERE post_id=p.id),'markdown' FROM legacy.posts p");
            check(before.equals(newRows("modern")),"Forward values differ");
        });
        db.queryForObject("SELECT setval('modern.posts_id_seq',(SELECT max(id) FROM modern.posts),true)",Long.class);
        System.out.println("PASS forward-import rows=2 normalizedValuesMatch=true");

        Files.writeString(live.resolve("cover-3.bin"),"updated-image-bytes");Files.writeString(live.resolve("cover-4.bin"),"new-image-bytes");
        tx.executeWithoutResult(status->{
            db.update("UPDATE modern.posts SET title='after-one',cover='cover-3.bin',tags='[\"db\",\"recovery\"]'::jsonb WHERE id=1");
            db.update("DELETE FROM modern.posts WHERE id=2");
            long id=db.queryForObject("INSERT INTO modern.posts(title,content,cover,tags,format) VALUES ('after-three','new-body','cover-4.bin','[\"new\"]','markdown') RETURNING id",Long.class);
            check(id==3,"Expected post-cutover id3");
        });
        check(before.size()==newRows("modern").size()&&!before.equals(newRows("modern")),"Same count must hide changes");
        check(db.queryForList("SELECT id FROM legacy.posts ORDER BY id",Long.class).equals(List.of(1L,2L)),"Old state");
        check(db.queryForList("SELECT id FROM modern.posts ORDER BY id",Long.class).equals(List.of(1L,3L)),"New state");
        check(db.queryForObject("SELECT title FROM legacy.posts WHERE id=1",String.class).equals("before-one"),"Stale edit");
        System.out.println("PASS stale-recovery countBoth=2 oldIds=[1, 2] currentIds=[1, 3] editLost=true deletedRowReturns=true newRowMissing=true");

        // Controlled single writer: no writes occur during snapshot/manifest capture.
        // This SQL table copy is not a pg_dump archive or a production backup implementation.
        db.execute("CREATE SCHEMA latest");db.execute("CREATE TABLE latest.posts AS TABLE modern.posts");
        var current=newRows("modern");check(current.equals(newRows("latest")),"Latest copy differs");
        db.update("UPDATE latest.posts SET format='blocks' WHERE id=1");
        boolean rejected=false;try{reverse("blocked_candidate");}catch(IllegalStateException e){rejected=e.getMessage().equals("Unsupported old format");}
        check(rejected&&db.queryForObject("SELECT count(*) FROM blocked_candidate.posts",Long.class)==0,"Lossy reverse must refuse before insert");
        db.update("UPDATE latest.posts SET format='markdown' WHERE id=1");reverse("rollback_candidate");
        check(current.equals(oldRows("rollback_candidate")),"Fresh reverse lost values");
        boolean collision=false;try{db.update("INSERT INTO rollback_candidate.posts(title,body,cover) VALUES ('probe','probe','cover-4.bin')");}catch(DataIntegrityViolationException e){collision=true;}
        check(collision,"Unadjusted sequence must collide with id1");
        db.queryForObject("SELECT setval('rollback_candidate.posts_id_seq',(SELECT max(id) FROM rollback_candidate.posts),true)",Long.class);
        long nextId=db.queryForObject("INSERT INTO rollback_candidate.posts(title,body,cover) VALUES ('after-recovery','probe','cover-4.bin') RETURNING id",Long.class);
        check(nextId==4,"Post recovery write");
        db.update("DELETE FROM rollback_candidate.posts WHERE id=?",nextId);
        check(current.equals(oldRows("rollback_candidate")),"Probe cleanup did not preserve state");
        System.out.println("PASS fresh-reverse normalizedValuesMatch=true incompatibleFormatRejected=true unadjustedSequenceCollision=true nextWriteId=4");

        var manifest=new TreeMap<String,String>();
        for(String key:db.queryForList("SELECT DISTINCT cover FROM latest.posts ORDER BY cover",String.class))manifest.put(key,hash(live.resolve(key)));
        check(invalidObjects(oldObjects,manifest).equals(List.of("cover-3.bin","cover-4.bin")),"Old object backup must miss current references");
        for(String key:manifest.keySet())Files.copy(live.resolve(key),restored.resolve(key));
        Files.writeString(restored.resolve("cover-3.bin"),"corrupt");check(invalidObjects(restored,manifest).equals(List.of("cover-3.bin")),"Hash mismatch must be detected");
        Files.copy(live.resolve("cover-3.bin"),restored.resolve("cover-3.bin"),StandardCopyOption.REPLACE_EXISTING);
        check(invalidObjects(restored,manifest).isEmpty(),"Restored object bytes differ");
        check(before.equals(oldRows("legacy"))&&current.equals(newRows("modern")),"Source state changed during recovery");
        System.out.println("PASS object-recovery oldBackupMissing=2 corruptionDetected=true restoredReferences=2 sourceRowsUnchanged=true");
        System.out.println("ALL 4 RECOVERY CHECKPOINTS PASSED. See ANSWERS.md for comparison and limits.");
    }
    public static void main(String[] args) throws Exception {
        String mode=args.length==0?"guided":args[0];if(!Set.of("guided","verify").contains(mode))throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")){System.out.println("예상: 이관 전후 글 수가 모두2면 복구해도 같을까요? 새 글/수정/삭제/파일/다음 쓰기를 확인합니다. Enter 실행 / q 종료");if(new Scanner(System.in).nextLine().strip().equalsIgnoreCase("q"))return;}
        Path root=Files.createTempDirectory("recovery-fixtures-");
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1").setPort(0).setServerConfig("shared_buffers","32MB").start()) {
            var ds=pg.getPostgresDatabase();try(var c=ds.getConnection()){System.out.printf("Spring=%s PostgreSQL=%s JDBC=%s%n",SpringVersion.getVersion(),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion());}
            new RecoveryLab(ds).run(root);
        } finally {try(var paths=Files.walk(root)){for(Path p:paths.sorted(Comparator.reverseOrder()).toList())Files.delete(p);}}
    }
}
