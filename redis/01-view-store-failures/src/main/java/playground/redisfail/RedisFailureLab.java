package playground.redisfail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import playground.redisfail.Views.Admission;
import playground.redisfail.Views.Mode;
import playground.redisfail.Views.Result;

/**
 * 조회수 중복 제거와 인기 글을 Redis에 둔 구조에서 Redis가 죽거나, 느려지거나, 재시작하거나, 꽉 차거나,
 * 네트워크 건너편에 있을 때 무슨 일이 생기는지 본다. 실제 redis-server와 임시 PostgreSQL을 쓴다.
 * guided: 실험마다 질문을 보여 주고 Enter를 기다린다. verify: 질문 없이 모두 실행하고 판정한다.
 */
public final class RedisFailureLab {
    private static boolean guided;
    private static Scanner in;

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0]))) {
            System.err.println("Usage: guided|verify");
            System.exit(2);
        }
        guided = args.length == 0 || args[0].equals("guided");
        in = new Scanner(System.in);
        String redis;
        try {
            redis = RedisProcess.BINARY + " (" + RedisProcess.VERSION + ")";
        } catch (ExceptionInInitializerError e) {
            System.err.println(e.getCause().getMessage());
            System.exit(1);
            return;
        }
        if (!pause("""
                같은 세션의 반복 조회를 Redis SET NX EX(24시간)로 거르고, 최근 순위를 날짜별 sorted set에 세는 구조에서
                Redis가 죽고, 느려지고, 재시작하고, 꽉 차고, 네트워크 건너편에 있을 때를 봅니다.
                redis-server: %s, 조회수 원본은 임시 PostgreSQL 18.4에 둡니다.""".formatted(redis))) return;
        try (var db = new Db()) {
            outage(db);
            slow(db);
            restart(db);
            memory(db);
            network(db);
        } catch (Quit q) {
            System.out.println("종료합니다. 임시 Redis와 DB를 정리했습니다.");
            return;
        }
        System.out.println("PASS experiments=5 realRedis=" + RedisProcess.VERSION + " realPostgres=true");
    }

    // ------------------------------------------------------------------ 1. Redis가 죽으면
    static void outage(Db db) throws Exception {
        if (!pause("""
                [1/5] 조회 요청이 10ms 간격으로 들어오는 중에 Redis를 kill -9로 죽이고 1초 뒤 다시 띄웁니다.
                그 1초 동안 요청은 얼마나 걸리고, 조회수는 어떻게 될까요? 연결이 끊긴 동안 명령을 바로 거부하는 설정
                (REJECT_COMMANDS, 블로그 운영 설정)과 Lettuce 기본값(재연결까지 쌓아 둠)을 비교합니다.""")) {
            throw new Quit();
        }
        for (boolean reject : List.of(true, false)) {
            db.reset();
            try (var redis = new RedisProcess("no"); var views = new Views(redis.port, db, reject)) {
                record Event(long atMs, long endMs, Admission admission, double ms) {}
                var events = new ArrayList<Event>();
                long t0 = System.nanoTime();
                long killAt = 1_000, restartAt = 2_000;
                long restartedAt = -1;
                boolean killed = false;
                Optional<List<String>> rankingDuringOutage = null;
                int i = 0, firstAfter = 0;
                while (true) {
                    long now = (System.nanoTime() - t0) / 1_000_000;
                    if (!killed && now >= killAt) {
                        redis.kill();
                        killed = true;
                    }
                    if (killed && restartedAt < 0 && now >= restartAt) {
                        redis.start();
                        restartedAt = (System.nanoTime() - t0) / 1_000_000;
                    }
                    if (restartedAt >= 0 && (firstAfter >= 20 || now > restartAt + 8_000)) break;
                    Result r = views.record(Mode.REDIS, 1 + i % Db.POSTS, "outage-" + reject + "-" + i);
                    i++;
                    events.add(new Event(now, (System.nanoTime() - t0) / 1_000_000, r.admission(), r.nanos() / 1e6));
                    if (restartedAt >= 0 && r.admission() == Admission.FIRST) firstAfter++;
                    if (killed && restartedAt < 0 && rankingDuringOutage == null) rankingDuringOutage = views.ranking();
                    Thread.sleep(10);
                }
                var before = events.stream().filter(e -> e.atMs() < killAt).toList();
                var down = events.stream().filter(e -> e.atMs() >= killAt && e.atMs() < restartAt).toList();
                // 재시작을 시작한 뒤에 보낸 요청은 모두 "재시작 뒤"로 센다(재시작 순간 보낸 요청 포함).
                var after = events.stream().filter(e -> e.atMs() >= restartAt).toList();
                long recoveredAt = after.stream()
                        .filter(e -> e.admission() == Admission.FIRST)
                        .mapToLong(Event::endMs)
                        .findFirst()
                        .orElse(-1);
                var afterMs = after.stream().limit(5).map(Event::ms).toList();
                long counted = events.stream().filter(e -> e.admission() == Admission.FIRST).count();
                long uncountedDown = down.stream().filter(e -> e.admission() == Admission.UNAVAILABLE).count();
                long uncountedAfter = after.stream().filter(e -> e.admission() == Admission.UNAVAILABLE).count();
                var downMs = down.stream().map(Event::ms).toList();
                long pg = db.totalCount();
                String mode = reject ? "rejectWhileDisconnected" : "lettuceDefault";
                check(before.stream().allMatch(e -> e.admission() == Admission.FIRST), mode + " before outage all counted");
                check(!down.isEmpty() && uncountedDown == down.size(), mode + " every request during the outage uncounted");
                check(recoveredAt >= 0, mode + " counts again after restart");
                check(pg == counted, mode + " PostgreSQL count " + pg + " == counted " + counted);
                check(rankingDuringOutage != null && rankingDuringOutage.isEmpty(), mode + " ranking unavailable while down");
                if (reject) check(max(downMs) < 50, mode + " fails fast, max " + max(downMs));
                else check(pct(downMs, 50) >= 250, mode + " waits for the command timeout, p50 " + pct(downMs, 50));
                System.out.printf("PASS outage client=%s requestsWhileDown=%d uncounted=%d latencyWhileDown p50=%.1fms max=%.1fms"
                                + " firstCountedAfterRestart=%dms uncountedAfterRestart=%d firstRequestsAfterRestart=%s counted=%d"
                                + " postgres=%d%n",
                        mode, down.size(), uncountedDown, pct(downMs, 50), max(downMs), recoveredAt - restartedAt,
                        uncountedAfter, after.stream().limit(5).map(e -> e.admission().name().charAt(0) + String.format("%.0f", e.ms())).toList(),
                        counted, pg);
            }
        }
    }

    // ------------------------------------------------------------------ 2. Redis가 느리면
    static void slow(Db db) throws Exception {
        if (!pause("""
                [2/5] Redis 응답만 늦춥니다(0 / 100 / 250 / 400ms). 명령 타임아웃은 300ms입니다. 새 세션 20개로 한 번씩
                조회한 뒤, 지연을 없애고 같은 세션으로 한 번 더 조회합니다. 250ms일 때 조회 한 번은 얼마나 걸릴까요?
                400ms일 때 두 번째 조회는 세어질까요?""")) {
            throw new Quit();
        }
        db.reset();
        try (var redis = new RedisProcess("no"); var proxy = new DelayProxy(redis.port);
                var views = new Views(proxy.port(), db, true)) {
            for (long delay : List.of(0L, 100L, 250L, 400L)) {
                proxy.delay(0, delay * 1_000);
                int n = 20, first = 0, unavailable = 0;
                var ms = new ArrayList<Double>();
                for (int k = 0; k < n; k++) {
                    Result r = views.record(Mode.REDIS, 1 + k, "slow-" + delay + "-" + k);
                    ms.add(r.nanos() / 1e6);
                    if (r.admission() == Admission.FIRST) first++;
                    if (r.admission() == Admission.UNAVAILABLE) unavailable++;
                }
                proxy.delay(0, 0);
                Thread.sleep(600); // 늦게 온 응답이 다 지나가게
                int firstAgain = 0, repeat = 0;
                for (int k = 0; k < n; k++) {
                    Admission a = views.record(Mode.REDIS, 1 + k, "slow-" + delay + "-" + k).admission();
                    if (a == Admission.FIRST) firstAgain++;
                    if (a == Admission.REPEAT) repeat++;
                }
                int lost = n - first - firstAgain;
                if (delay < 300) {
                    check(first == n && lost == 0, "delay " + delay + " all counted once");
                    // SET NX, ZINCRBY, EXPIRE: three commands, each under its own 300ms timeout.
                    if (delay > 0) check(pct(ms, 50) >= 2.8 * delay, "delay " + delay + " stacks per command: " + pct(ms, 50));
                } else {
                    check(first == 0 && unavailable == n, "delay " + delay + " every first view timed out");
                    check(repeat == n && lost == n, "delay " + delay + " the keys were set anyway: lost " + lost);
                    check(max(ms) < delay, "delay " + delay + " bounded by the command timeout, max " + max(ms));
                }
                System.out.printf("PASS slow responseDelay=%dms first: counted=%d timedOut=%d p50=%.1fms max=%.1fms"
                                + " | again without delay: repeat=%d counted=%d | neverCounted=%d%n",
                        delay, first, unavailable, pct(ms, 50), max(ms), repeat, firstAgain, lost);
            }
            check(db.totalCount() == 60, "PostgreSQL counted only the rounds under the timeout");
        }
    }

    // ------------------------------------------------------------------ 3. Redis가 재시작하면
    static void restart(Db db) throws Exception {
        if (!pause("""
                [3/5] 새 세션 2,000개를 조회해 키를 만든 직후 Redis를 kill -9로 죽이고 같은 디렉터리로 다시 띄웁니다.
                AOF 없음 / appendfsync everysec / always에서 중복 제거 키와 순위는 얼마나 남을까요? 같은 세션으로 다시
                조회하면 몇 개가 한 번 더 세어질까요? 키를 쓰는 비용도 비교합니다.""")) {
            throw new Quit();
        }
        int n = 2_000;
        for (String aof : List.of("no", "everysec", "always")) {
            db.reset();
            try (var redis = new RedisProcess(aof); var views = new Views(redis.port, db, true)) {
                var ms = new ArrayList<Double>();
                int counted = 0;
                for (int k = 0; k < n; k++) {
                    Result r = views.record(Mode.REDIS, 1 + k % Db.POSTS, "aof-" + aof + "-" + k);
                    ms.add(r.nanos() / 1e6);
                    if (r.admission() == Admission.FIRST) counted++;
                }
                redis.kill();
                redis.start();
                awaitReconnect(views);
                long keys = redis.countKeys("view:*");
                double ranked = redis.admin().zrangeWithScores(Views.dayKey(), 0, -1).stream()
                        .mapToDouble(s -> s.getScore())
                        .sum();
                int doubled = 0;
                for (int k = 0; k < n; k++) {
                    if (views.record(Mode.REDIS, 1 + k % Db.POSTS, "aof-" + aof + "-" + k).admission() == Admission.FIRST) {
                        doubled++;
                    }
                }
                long pg = db.totalCount();
                check(counted == n, aof + " all first views counted");
                check(pg == counted + doubled, aof + " PostgreSQL " + pg);
                if (aof.equals("no")) check(keys == 0 && doubled == n, aof + " nothing survives: keys " + keys);
                else check(keys == n && doubled == 0 && ranked == n, aof + " everything survives: keys " + keys + " ranked " + ranked);
                System.out.printf("PASS restart aof=%s writeP50=%.2fms writeP99=%.2fms keysAfterKill=%d/%d rankingAfterKill=%.0f"
                                + " countedAgain=%d postgres=%d%n",
                        aof, pct(ms, 50), pct(ms, 99), keys, n, ranked, doubled, pg);
            }
        }
    }

    // ------------------------------------------------------------------ 4. Redis가 꽉 차면
    static void memory(Db db) throws Exception {
        if (!pause("""
                [4/5] Redis 메모리 한도를 지금 쓰는 양 + 1MB로 줄이고 새 세션 30,000개를 조회합니다(글 60개, 소수 글에 쏠림).
                그다음 처음 5,000개 세션으로 다시 조회합니다. allkeys-lru(블로그 운영 설정) / volatile-ttl / noeviction에서
                무엇이 지워지고, 몇 개가 한 번 더 세어지거나 아예 세어지지 않을까요?""")) {
            throw new Quit();
        }
        int n = 30_000, replay = 5_000;
        long[] posts = new long[n];
        var random = new Random(42);
        for (int k = 0; k < n; k++) posts[k] = 1 + (long) Math.floor(Db.POSTS * Math.pow(random.nextDouble(), 2));
        for (String policy : List.of("allkeys-lru", "volatile-ttl", "noeviction")) {
            try (var redis = new RedisProcess("no"); var views = new Views(redis.port, db, true)) {
                views.redisOnly(0, "warm-up");
                long baseline = Long.parseLong(redis.info("memory").get("used_memory"));
                redis.admin().configSet("maxmemory", Long.toString(baseline + 1_000_000));
                redis.admin().configSet("maxmemory-policy", policy);
                int counted = 0, refused = 0;
                for (int k = 0; k < n; k++) {
                    Admission a = views.redisOnly(posts[k], "mem-" + k);
                    if (a == Admission.FIRST) counted++;
                    if (a == Admission.UNAVAILABLE) refused++;
                }
                long evicted = Long.parseLong(redis.info("stats").get("evicted_keys"));
                long keys = redis.countKeys("view:*");
                boolean ranking = redis.admin().exists(Views.dayKey()) == 1;
                double ranked = ranking
                        ? redis.admin().zrangeWithScores(Views.dayKey(), 0, -1).stream().mapToDouble(s -> s.getScore()).sum()
                        : 0;
                int doubled = 0, refusedAgain = 0;
                for (int k = 0; k < replay; k++) {
                    Admission a = views.redisOnly(posts[k], "mem-" + k);
                    if (a == Admission.FIRST) doubled++;
                    if (a == Admission.UNAVAILABLE) refusedAgain++;
                }
                switch (policy) {
                    case "noeviction" -> {
                        check(refused > 0 && evicted == 0, policy + " refuses writes instead of evicting");
                        check(doubled == 0, policy + " never counts a kept session twice");
                    }
                    case "volatile-ttl" -> {
                        check(refused == 0 && evicted > 0 && doubled > 0, policy + " evicts dedup keys and counts again");
                        check(ranking && ranked == counted + 1, policy + " keeps the whole ranking (longer TTL): " + ranked);
                    }
                    default -> check(refused == 0 && evicted > 0 && doubled > 0, policy + " evicts and counts again");
                }
                System.out.printf("PASS memory policy=%s firstPass: counted=%d refused=%d evicted=%d viewKeysLeft=%d rankingKey=%s"
                                + " rankingSum=%.0f | replay first %d: countedAgain=%d refused=%d%n",
                        policy, counted, refused, evicted, keys, ranking ? "kept" : "evicted", ranked, replay, doubled,
                        refusedAgain);
            }
        }
    }

    // ------------------------------------------------------------------ 5. 네트워크를 건너면
    static void network(Db db) throws Exception {
        if (!pause("""
                [5/5] Redis와 PostgreSQL 앞에 같은 왕복 지연(0 / 1 / 5ms)을 넣고 새 세션의 조회 기록 시간을 잽니다.
                postgres(한 트랜잭션), redis(SET NX → PostgreSQL → ZINCRBY → EXPIRE), redis-script(판정·순위·만료를
                Lua 스크립트 한 번으로) 중 무엇이 얼마나 느려질까요?""")) {
            throw new Quit();
        }
        db.reset();
        try (var redis = new RedisProcess("no"); var proxy = new DelayProxy(redis.port);
                var views = new Views(proxy.port(), db, true)) {
            int warm = 50, n = 300, seq = 0;
            for (long oneWayMicros : List.of(0L, 500L, 2_500L)) {
                proxy.delay(oneWayMicros, oneWayMicros);
                db.proxy.delay(oneWayMicros, oneWayMicros);
                var p50 = new java.util.LinkedHashMap<Mode, Double>();
                var trips = new java.util.LinkedHashMap<Mode, String>();
                for (Mode mode : Mode.values()) {
                    var ms = new ArrayList<Double>();
                    long redisBefore = 0, pgBefore = 0;
                    for (int k = 0; k < warm + n; k++) {
                        if (k == warm) {
                            redisBefore = proxy.upChunks.get();
                            pgBefore = db.proxy.upChunks.get();
                        }
                        Result r = views.record(mode, 1 + k % Db.POSTS, "rtt-" + seq++);
                        check(r.admission() == Admission.FIRST, mode + " first view counted");
                        if (k >= warm) ms.add(r.nanos() / 1e6);
                    }
                    p50.put(mode, pct(ms, 50));
                    trips.put(mode, "redis " + Math.round((proxy.upChunks.get() - redisBefore) / (double) n)
                            + " + postgres " + Math.round((db.proxy.upChunks.get() - pgBefore) / (double) n));
                }
                if (oneWayMicros == 0L) {
                    System.out.println("     roundTripsPerView " + trips);
                }
                double rttMs = 2 * oneWayMicros / 1000.0;
                if (oneWayMicros == 2_500L) {
                    double extra = p50.get(Mode.REDIS) - p50.get(Mode.POSTGRES);
                    check(extra >= 1.4 * rttMs, "redis pays two more round trips than postgres: +" + extra);
                    check(Math.abs(p50.get(Mode.REDIS_SCRIPT) - p50.get(Mode.POSTGRES)) <= 0.8 * rttMs,
                            "redis-script pays as many round trips as postgres");
                }
                System.out.printf("PASS network rtt=%.0fms recordP50 postgres=%.2fms redis=%.2fms redisScript=%.2fms%n",
                        rttMs, p50.get(Mode.POSTGRES), p50.get(Mode.REDIS), p50.get(Mode.REDIS_SCRIPT));
            }
            db.proxy.delay(0, 0);
        }
    }

    // ------------------------------------------------------------------ helpers
    private static void awaitReconnect(Views views) throws Exception {
        long until = System.nanoTime() + 15_000_000_000L;
        while (System.nanoTime() < until) {
            if (views.ranking().isPresent()) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("FAIL client did not reconnect");
    }

    static double pct(List<Double> values, int p) {
        var sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        if (sorted.isEmpty()) return 0;
        return sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(p / 100.0 * sorted.size()) - 1));
    }

    static double max(List<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).max().orElse(0);
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
