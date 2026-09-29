package playground.outbox;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class OrderLab {
    static DataSource ds;
    record Event(String id, long version, String title, boolean deleted) { }
    interface Work<T> { T run(Connection c) throws Exception; }
    static void check(boolean ok,String reason){if(!ok)throw new IllegalStateException(reason);}
    static <T>T tx(Work<T>w)throws Exception{try(var c=ds.getConnection()){c.setAutoCommit(false);try{T v=w.run(c);c.commit();return v;}catch(Exception e){c.rollback();throw e;}}}
    static int sql(Connection c,String text,Object...args)throws Exception{try(var p=c.prepareStatement(text)){for(int i=0;i<args.length;i++)p.setObject(i+1,args[i]);return p.executeUpdate();}}
    static void reset()throws Exception{tx(c->{sql(c,"TRUNCATE inbox,projection,counter");return null;});}
    static String apply(Event e,boolean guarded)throws Exception {
        return tx(c->{
            if(sql(c,"INSERT INTO inbox VALUES ('view',?) ON CONFLICT DO NOTHING",e.id)==0)return "DUPLICATE";
            int changed=sql(c,"INSERT INTO projection VALUES (1,?,?,?) ON CONFLICT(id) DO UPDATE SET version=EXCLUDED.version,title=EXCLUDED.title,deleted=EXCLUDED.deleted"
                +(guarded?" WHERE projection.version < EXCLUDED.version":""),e.version,e.title,e.deleted);
            return changed==1?"APPLIED":"STALE";
        });
    }
    static String row()throws Exception{try(var c=ds.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT version||':'||coalesce(title,'<null>')||':'||deleted FROM projection WHERE id=1")){check(r.next(),"Missing projection");return r.getString(1);}}
    static long count(String table)throws Exception{try(var c=ds.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT count(*) FROM "+table)){r.next();return r.getLong(1);}}
    static long visible()throws Exception{try(var c=ds.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT count(*) FROM projection WHERE NOT deleted")){r.next();return r.getLong(1);}}
    static void scenario()throws Exception{
        Event old=new Event("E1",1,"old",false),fresh=new Event("E2",2,"new",false),deleted=new Event("E3",3,null,true);
        reset();check(apply(fresh,false).equals("APPLIED")&&apply(old,false).equals("APPLIED"),"Naive path");
        check(row().equals("1:old:false")&&count("inbox")==2,"Expected naive regression");
        System.out.println("PASS inbox-only delivery=E2,E1 final=1:old:false inbox=2 regressionObserved=true");

        reset();check(apply(fresh,true).equals("APPLIED")&&apply(old,true).equals("STALE"),"Guarded path");
        check(row().equals("2:new:false")&&count("inbox")==2,"Latest snapshot not retained");
        System.out.println("PASS version-guard delivery=E2,E1 final=2:new:false inbox=2 oldResult=STALE");

        check(apply(fresh,true).equals("DUPLICATE")&&count("inbox")==2&&row().equals("2:new:false"),"Duplicate changed state");
        System.out.println("PASS same-event replay=E2 result=DUPLICATE final=2:new:false inbox=2");

        check(apply(deleted,true).equals("APPLIED"),"Delete failed");
        // A new event ID forces version protection rather than Inbox to do the work.
        check(apply(new Event("E-old-late",1,"old",false),true).equals("STALE"),"Delete resurrected");
        check(row().equals("3:<null>:true")&&visible()==0&&count("inbox")==4,"Tombstone failed");
        System.out.println("PASS delete-tombstone final=3:<null>:true visible=0 lateOldResult=STALE inbox=4");

        tx(c->{sql(c,"DELETE FROM projection WHERE id=1");return null;});
        check(apply(new Event("E-other-late",2,"new",false),true).equals("APPLIED"),"Expected absent-row insert");
        check(row().equals("2:new:false")&&visible()==1&&count("inbox")==5,"Expected resurrection after forgetting version");
        System.out.println("PASS hard-delete-counterexample final=2:new:false visible=1 resurrectionObserved=true");

        reset();tx(c->{sql(c,"INSERT INTO counter VALUES (1,0,0)");return null;});
        int applied2=tx(c->sql(c,"UPDATE counter SET version=?,total=total+? WHERE id=1 AND version<?",2,7,2));
        int skipped1=tx(c->sql(c,"UPDATE counter SET version=?,total=total+? WHERE id=1 AND version<?",1,3,1));
        try(var c=ds.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT version,total FROM counter WHERE id=1")){
            check(r.next()&&r.getLong(1)==2&&r.getInt(2)==7&&applied2==1&&skipped1==0,"Delta counterexample");}
        System.out.println("PASS delta-counterexample deltaV2=7 deltaV1=3 guardedTotal=7 intendedTotal=10 lostDeltaObserved=true");
    }
    public static void main(String[]args)throws Exception{
        check(args.length<=1&&(args.length==0||Set.of("guided","verify").contains(args[0])),"guided|verify");
        if(args.length==0||args[0].equals("guided")){
            System.out.println("Outbox 05: E2 다음 E1을 받으면? 삭제한 행을 지우면? EXERCISES.md에 예상 후 Enter/q.");
            var in=new Scanner(System.in);if(!in.hasNextLine()||in.nextLine().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setPort(0).start()){
            ds=pg.getPostgresDatabase();tx(c->{
                sql(c,"CREATE TABLE inbox(consumer_name text,event_id text,PRIMARY KEY(consumer_name,event_id))");
                sql(c,"CREATE TABLE projection(id bigint PRIMARY KEY,version bigint NOT NULL,title text,deleted boolean NOT NULL)");
                sql(c,"CREATE TABLE counter(id bigint PRIMARY KEY,version bigint NOT NULL,total integer NOT NULL)");return null;});
            try(var c=ds.getConnection();var s=c.createStatement();var r=s.executeQuery("SHOW server_version")){r.next();System.out.println("PostgreSQL="+r.getString(1)+" Kafka=not-started eventOrder=explicit-calls");}
            scenario();
        }
        System.out.println("ALL 6 SCENARIOS PASSED. Counterexamples intentionally demonstrate incorrect outcomes.");
    }
}
