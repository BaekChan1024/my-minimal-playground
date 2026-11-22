package playground.sagaiso;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import playground.sagaiso.SummarySaga.Options;
import playground.sagaiso.SummarySaga.Outcome;
import playground.sagaiso.SummarySaga.Step;

/**
 * Saga 단계 사이에는 격리가 없다. 두 Saga(또는 Saga와 조회·글 수정)의 단계가 섞일 때 생기는 이상 현상을
 * 정해 둔 실행 순서로 재현하고, 대응책을 하나씩 켜서 비교한다. 이어서 같은 조건을 동시에 실행해 빈도를 센다.
 * guided: 실험마다 질문을 보여 주고 Enter를 기다린다. verify: 질문 없이 모두 실행하고 판정한다.
 */
public final class IsolationLab {
    private static boolean guided;
    private static Scanner in;
    private static final String AUTHOR = "author";

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0]))) {
            throw new IllegalArgumentException("guided|verify");
        }
        guided = args.length == 0 || args[0].equals("guided");
        in = new Scanner(System.in);
        if (!pause("""
                Saga의 각 단계는 따로 커밋되므로, 다른 Saga나 조회가 그 사이의 중간 상태를 봅니다.
                "AI 요약 게시" Saga로 네 가지 이상 현상을 재현하고 대응책을 비교합니다. 임시 PostgreSQL을 사용합니다.""")) return;
        try (var store = new Store()) {
            dirtyRead(store);
            lostUpdate(store);
            staleRead(store);
            quota(store);
        } catch (Quit q) {
            System.out.println("종료합니다. 임시 DB를 정리했습니다.");
            return;
        }
        System.out.println("PASS experiments=4 realPostgres=true");
    }

    // ------------------------------------------------------------------ 1. 더티 리드: 되돌려질 요약을 남이 본다
    static void dirtyRead(Store store) throws Exception {
        if (!pause("""
                [1/4] 요약을 게시한 뒤 검수에서 떨어져 되돌려지는 Saga가 있습니다. 게시와 되돌림 사이에 독자가 글을 열면
                무엇을 볼까요? 검수를 앞으로 옮기거나, 끝날 때까지 감추면 어떻게 될까요?""")) throw new Quit();
        var variants = new LinkedHashMap<String, Options>();
        variants.put("naive", Options.naive());
        variants.put("reorder", Options.naive().withReorder());
        variants.put("hidden", Options.naive().withHidden());
        for (var v : variants.entrySet()) {
            try (var w = store.new World()) {
                seed(w, 1, 1_000_000);
                var a = new SummarySaga(w, v.getValue(), "A", AUTHOR, 1, 100, true);
                a.runUntil(Step.MODERATE);
                String seen = visible(w, 1);
                a.runToEnd();
                int dirty = seen != null && seen.equals("A") && a.outcome == Outcome.COMPENSATED ? 1 : 0;
                check(dirty == (v.getKey().equals("naive") ? 1 : 0), "dirty " + v.getKey());
                check(a.outcome == Outcome.COMPENSATED && visible(w, 1) == null, "A compensated " + v.getKey());
                var load = dirtyLoad(store, v.getValue());
                if (!v.getKey().equals("naive")) check(load[0] == 0, "no dirty reads under load with " + v.getKey());
                System.out.println("PASS dirtyRead variant=" + v.getKey() + " schedule=[A until MODERATE, reader, A rest]"
                        + " readerSaw=" + seen + " A=" + a.outcome + " steps=" + a.log
                        + " | load: sagas=200 flagged=" + load[2] + " listReads=" + load[1] + " retractedSeen=" + load[0]);
            }
        }
    }

    /** 글 200개에 Saga 하나씩(30%는 검수 탈락), 독자 스레드 하나가 계속 읽는다. {되돌려진 요약을 본 횟수, 읽은 횟수, 탈락 Saga 수}. */
    private static long[] dirtyLoad(Store store, Options o) throws Exception {
        try (var w = store.new World()) {
            int n = 200;
            seed(w, n, 1_000_000_000L);
            var sagas = new ArrayList<SummarySaga>();
            for (int i = 1; i <= n; i++) sagas.add(new SummarySaga(w, o, "S" + i, AUTHOR, i, 100, i % 10 < 3));
            var seenBy = ConcurrentHashMap.<String>newKeySet();
            var seenCount = new long[] {0, 0};
            var stop = new AtomicBoolean();
            var reader = Thread.ofPlatform().start(() -> {
                try {
                    // 한 번에 모든 글의 보이는 요약을 읽는다(목록 화면 한 번 열기에 해당).
                    while (!stop.get()) {
                        w.content(c -> {
                            try (var ps = c.prepareStatement("SELECT summary_saga FROM post WHERE summary_visible");
                                    var rs = ps.executeQuery()) {
                                while (rs.next()) seenBy.add(rs.getString(1));
                            }
                            return null;
                        });
                        seenCount[1]++;
                    }
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            });
            runConcurrently(sagas);
            stop.set(true);
            reader.join();
            long retracted = 0;
            for (var s : sagas) if (s.outcome == Outcome.COMPENSATED && seenBy.contains(s.id)) retracted++;
            long flagged = sagas.stream().filter(s -> s.flagged).count();
            return new long[] {retracted, seenCount[1], flagged};
        }
    }

    // ------------------------------------------------------------------ 2. 갱신 손실: 보상이 남의 결과를 지운다
    static void lostUpdate(Store store) throws Exception {
        if (!pause("""
                [2/4] 같은 글에 Saga A와 B가 차례로 요약을 게시합니다. A가 나중에 검수에서 떨어져 게시를 되돌리면
                B의 요약은 어떻게 될까요? "내 것일 때만 되돌리기"와 "글에 작업 중 표시"는 각각 무엇을 막을까요?""")) throw new Quit();
        var variants = new LinkedHashMap<String, Options>();
        variants.put("naive", Options.naive());
        variants.put("casUndo", Options.naive().withCasUndo());
        variants.put("postLock", Options.naive().withPostLock());
        for (var v : variants.entrySet()) {
            String name = v.getKey();
            // 순서 1: A 게시 → B 끝까지(성공) → A 검수 탈락 후 보상
            String one;
            try (var w = store.new World()) {
                seed(w, 1, 1_000_000);
                var a = new SummarySaga(w, v.getValue(), "A", AUTHOR, 1, 100, true);
                var b = new SummarySaga(w, v.getValue(), "B", AUTHOR, 1, 100, false);
                a.runUntil(Step.MODERATE);
                b.runToEnd();
                a.runToEnd();
                String fin = summarySaga(w, 1);
                one = "A=" + a.outcome + " B=" + b.outcome + (b.reason == null ? "" : "(" + b.reason + ")") + " final=" + fin;
                switch (name) {
                    case "naive" -> check(b.outcome == Outcome.COMPLETED && fin == null, "naive loses B");
                    case "casUndo" -> check(b.outcome == Outcome.COMPLETED && "B".equals(fin), "cas keeps B");
                    default -> check(b.outcome == Outcome.COMPENSATED && "BUSY".equals(b.reason) && fin == null, "lock rejects B");
                }
            }
            // 순서 2: 둘 다 검수 탈락. A 게시 → B 게시 → A 보상 → B 보상
            String two;
            try (var w = store.new World()) {
                seed(w, 1, 1_000_000);
                var a = new SummarySaga(w, v.getValue(), "A", AUTHOR, 1, 100, true);
                var b = new SummarySaga(w, v.getValue(), "B", AUTHOR, 1, 100, true);
                a.runUntil(Step.MODERATE);
                b.runUntil(Step.MODERATE);
                a.runToEnd();
                b.runToEnd();
                String fin = summarySaga(w, 1);
                two = "A=" + a.outcome + " B=" + b.outcome + (b.reason == null ? "" : "(" + b.reason + ")") + " final=" + fin;
                if (name.equals("postLock")) check(fin == null, "lock leaves nothing");
                else check("A".equals(fin), name + " resurrects A");
            }
            var load = lostLoad(store, v.getValue());
            if (name.equals("postLock")) check(load[0] == 0, "no anomalies under load with postLock");
            System.out.println("PASS lostUpdate variant=" + name + " order1=[" + one + "] order2=[" + two + "]"
                    + " | load: posts=100 sagas=200 anomalies=" + load[0] + " busy=" + load[1] + " completed=" + load[2]);
        }
    }

    /**
     * 글 100개에 Saga 2개씩(각 30% 검수 탈락)을 동시에. 이상: 최종 요약이 보상된 Saga의 것이거나,
     * 완료된 Saga가 있는데 최종 요약이 완료된 Saga의 것이 아닌 글. {이상 글 수, BUSY로 거절된 Saga 수, 완료 Saga 수}.
     */
    private static long[] lostLoad(Store store, Options o) throws Exception {
        try (var w = store.new World()) {
            int posts = 100;
            seed(w, posts, 1_000_000_000L);
            var sagas = new ArrayList<SummarySaga>();
            var rnd = new java.util.Random(7);
            for (int p = 1; p <= posts; p++) {
                for (int k = 0; k < 2; k++) sagas.add(new SummarySaga(w, o, "P" + p + "S" + k, AUTHOR, p, 100, rnd.nextInt(10) < 3));
            }
            runConcurrently(sagas);
            var byId = new HashMap<String, SummarySaga>();
            for (var s : sagas) byId.put(s.id, s);
            long anomalies = 0;
            for (int p = 1; p <= posts; p++) {
                String fin = summarySaga(w, p);
                final int post = p;
                boolean anyCompleted = sagas.stream().anyMatch(s -> s.post == post && s.outcome == Outcome.COMPLETED);
                boolean finFromCompensated = fin != null && byId.get(fin).outcome != Outcome.COMPLETED;
                boolean completedLost = anyCompleted && (fin == null || byId.get(fin).outcome != Outcome.COMPLETED);
                if (finFromCompensated || completedLost) anomalies++;
            }
            long busy = sagas.stream().filter(s -> "BUSY".equals(s.reason)).count();
            long completed = sagas.stream().filter(s -> s.outcome == Outcome.COMPLETED).count();
            return new long[] {anomalies, busy, completed};
        }
    }

    // ------------------------------------------------------------------ 3. 오래된 읽기: 읽은 뒤 바뀐 글에 옛 요약을 붙인다
    static void staleRead(Store store) throws Exception {
        if (!pause("""
                [3/4] Saga가 글을 읽고 요약을 만드는 사이에 저자가 글을 고칩니다. 게시된 요약은 어느 버전의 글을 요약한 것일까요?
                게시할 때 버전을 다시 확인하거나, 작업 중 표시로 수정을 막으면 무엇이 달라질까요?""")) throw new Quit();
        var variants = new LinkedHashMap<String, Options>();
        variants.put("naive", Options.naive());
        variants.put("reread", Options.naive().withReread());
        variants.put("postLock", Options.naive().withPostLock());
        for (var v : variants.entrySet()) {
            String name = v.getKey();
            try (var w = store.new World()) {
                seed(w, 1, 1_000_000);
                var a = new SummarySaga(w, v.getValue(), "A", AUTHOR, 1, 100, false);
                a.runUntil(Step.ATTACH);
                boolean edited = edit(w, 1, "고친 본문", v.getValue().postLock());
                a.runToEnd();
                var row = postRow(w, 1);
                boolean stale = row.get("summary_saga") != null && !row.get("summary_version").equals(row.get("version"));
                switch (name) {
                    case "naive" -> check(edited && stale && a.outcome == Outcome.COMPLETED, "naive stale");
                    case "reread" -> check(edited && !stale && a.outcome == Outcome.COMPENSATED && "STALE".equals(a.reason)
                            && row.get("summary_saga") == null, "reread refuses");
                    default -> check(!edited && !stale && a.outcome == Outcome.COMPLETED, "lock blocks edit");
                }
                System.out.println("PASS staleRead variant=" + name + " schedule=[A until ATTACH, author edit, A rest]"
                        + " editAccepted=" + edited + " A=" + a.outcome + (a.reason == null ? "" : "(" + a.reason + ")")
                        + " post.version=" + row.get("version") + " summary.version=" + row.get("summary_version")
                        + " staleSummary=" + stale);
            }
        }
    }

    // ------------------------------------------------------------------ 4. 한도: 예약(비관적)과 확인만(낙관적)
    static void quota(Store store) throws Exception {
        if (!pause("""
                [4/4] 한도 1,000인 저자의 600짜리 요청 두 개가 겹칩니다. 시작할 때 예약하는 방식과, 잔량만 보고 끝날 때 더하는 방식은
                A의 요약 생성이 실패할 때와 둘 다 성공할 때 각각 어떻게 다를까요?""")) throw new Quit();
        var variants = new LinkedHashMap<String, Options>();
        variants.put("reserve", Options.naive());
        variants.put("optimistic", Options.naive().optimisticQuota());
        for (var v : variants.entrySet()) {
            String name = v.getKey();
            String[] results = new String[2];
            for (int sc = 0; sc < 2; sc++) {
                boolean aFails = sc == 0;
                try (var w = store.new World()) {
                    seedPosts(w, 2);
                    quotaRow(w, 1000);
                    // A가 실패한다면 요약 생성(공급자 호출)에서 실패한다. 토큰을 쓰기 전이라 예약을 돌려준다.
                    var a = new SummarySaga(w, v.getValue(), "A", AUTHOR, 1, 600, false, aFails);
                    var b = new SummarySaga(w, v.getValue(), "B", AUTHOR, 2, 600, false);
                    a.runUntil(Step.GENERATE);
                    b.runToEnd();
                    a.runToEnd();
                    long used = used(w);
                    results[sc] = (aFails ? "aFails" : "bothOk") + ": A=" + a.outcome + " B=" + b.outcome
                            + (b.reason == null ? "" : "(" + b.reason + ")") + " used=" + used;
                    boolean reserve = name.equals("reserve");
                    if (aFails) {
                        check(reserve ? b.outcome == Outcome.REJECTED && used == 0 : b.outcome == Outcome.COMPLETED && used == 600,
                                name + " when A fails");
                    } else {
                        check(reserve ? b.outcome == Outcome.REJECTED && used == 600 : b.outcome == Outcome.COMPLETED && used == 1200,
                                name + " when both succeed");
                    }
                }
            }
            var load = quotaLoad(store, v.getValue());
            if (name.equals("reserve")) check(load[1] == 0, "reservation never overruns");
            System.out.println("PASS quota variant=" + name + " " + results[0] + " | " + results[1]
                    + " | load: authors=50 sagas=150 falseRejections=" + load[0] + " overrunAuthors=" + load[1]
                    + " completed=" + load[2]);
        }
    }

    /**
     * 저자 50명(한도 1,000)이 600짜리 요청 3개씩(고정 시드로 약 30%가 요약 생성에서 실패)을 동시에. {거절됐지만 끝나고 보니 들어갈 자리가 있던 Saga 수,
     * 한도를 넘긴 저자 수, 완료 Saga 수}.
     */
    private static long[] quotaLoad(Store store, Options o) throws Exception {
        try (var w = store.new World()) {
            int authors = 50;
            seedPosts(w, authors * 3);
            w.usage(c -> {
                for (int i = 1; i <= authors; i++) Store.update(c, "INSERT INTO quota(author, lim) VALUES (?, 1000)", "a" + i);
                return null;
            });
            var sagas = new ArrayList<SummarySaga>();
            var rnd = new java.util.Random(11);
            long post = 1;
            for (int i = 1; i <= authors; i++) {
                for (int k = 0; k < 3; k++) {
                    sagas.add(new SummarySaga(w, o, "a" + i + "S" + k, "a" + i, post++, 600, false, rnd.nextInt(10) < 3));
                }
            }
            runConcurrently(sagas);
            var usedBy = new HashMap<String, Long>();
            w.usage(c -> {
                try (var ps = c.prepareStatement("SELECT author, used FROM quota"); var rs = ps.executeQuery()) {
                    while (rs.next()) usedBy.put(rs.getString(1), rs.getLong(2));
                }
                return null;
            });
            long falseRejections = sagas.stream()
                    .filter(s -> s.outcome == Outcome.REJECTED && usedBy.get(s.author) + s.tokens <= 1000).count();
            long overrun = usedBy.values().stream().filter(u -> u > 1000).count();
            long completed = sagas.stream().filter(s -> s.outcome == Outcome.COMPLETED).count();
            return new long[] {falseRejections, overrun, completed};
        }
    }

    // ------------------------------------------------------------------ helpers
    /** 각 Saga를 자기 스레드에서 한 단계씩, 단계 사이에 0~2ms 쉬며 실행한다(서비스 사이 호출 지연 흉내). */
    private static void runConcurrently(List<SummarySaga> sagas) throws Exception {
        try (var pool = Executors.newFixedThreadPool(32)) {
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (var s : sagas) {
                futures.add(pool.submit(() -> {
                    while (s.step()) Thread.sleep(ThreadLocalRandom.current().nextInt(3));
                    return null;
                }));
            }
            for (var f : futures) f.get(120, TimeUnit.SECONDS);
        }
    }

    private static void seed(Store.World w, int posts, long limit) throws SQLException {
        seedPosts(w, posts);
        quotaRow(w, limit);
    }

    private static void seedPosts(Store.World w, int posts) throws SQLException {
        w.content(c -> {
            for (long p = 1; p <= posts; p++) Store.update(c, "INSERT INTO post(id, body) VALUES (?, ?)", p, "본문 " + p);
            return null;
        });
    }

    private static void quotaRow(Store.World w, long limit) throws SQLException {
        w.usage(c -> Store.update(c, "INSERT INTO quota(author, lim) VALUES (?, ?)", AUTHOR, limit));
    }

    /** 독자가 보는 요약의 주인(감춰진 요약은 보이지 않는다). */
    private static String visible(Store.World w, long post) throws SQLException {
        return w.content(c -> Store.one(c, "SELECT summary_saga FROM post WHERE id = ? AND summary_visible", post));
    }

    private static String summarySaga(Store.World w, long post) throws SQLException {
        return w.content(c -> Store.one(c, "SELECT summary_saga FROM post WHERE id = ?", post));
    }

    private static Map<String, String> postRow(Store.World w, long post) throws SQLException {
        return w.content(c -> {
            var row = new HashMap<String, String>();
            try (var ps = c.prepareStatement("SELECT version, summary_saga, summary_version FROM post WHERE id = ?")) {
                ps.setLong(1, post);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    row.put("version", rs.getString(1));
                    row.put("summary_saga", rs.getString(2));
                    row.put("summary_version", rs.getString(3));
                }
            }
            return row;
        });
    }

    /** 저자의 글 수정. respectLock이면 작업 중 표시가 있는 글은 고치지 못한다. */
    private static boolean edit(Store.World w, long post, String body, boolean respectLock) throws SQLException {
        return w.content(c -> Store.update(c, "UPDATE post SET body = ?, version = version + 1 WHERE id = ?"
                + (respectLock ? " AND locked_by IS NULL" : ""), body, post) == 1);
    }

    private static long used(Store.World w) throws SQLException {
        return Long.parseLong(w.usage(c -> Store.one(c, "SELECT used FROM quota WHERE author = ?", AUTHOR)));
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException("FAIL " + message);
    }

    private static boolean pause(String question) {
        System.out.println();
        System.out.println(question);
        if (!guided) return true;
        System.out.print("Enter로 실행, q로 종료 > ");
        if (!in.hasNextLine()) return false;
        return !in.nextLine().trim().equalsIgnoreCase("q");
    }

    private static final class Quit extends RuntimeException {}
}
