package playground.sourcefailure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import playground.sourcefailure.FailureConsumer.Policy;

/**
 * zero payload 소비자가 원본 조회에 의존할 때 생기는 문제를 다섯 조건에서 확인한다.
 * guided: 실험마다 질문을 보여 주고 Enter를 기다린다. verify: 질문 없이 모두 실행하고 판정한다.
 */
public final class SourceFailureLab {
    private static final Duration WAIT = Duration.ofSeconds(20);
    /** 실험 5의 그룹 소비자 설정. 바꿔 보기 과제에서 이 값만 바꾼다. */
    static final String MAX_POLL_INTERVAL_MS = "2000";
    private static boolean guided;
    private static Scanner in;

    /** guided 실행 중 q를 누르면 자원을 닫고 끝내기 위한 신호. */
    private static final class Quit extends RuntimeException {}

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0]))) {
            throw new IllegalArgumentException("guided|verify");
        }
        guided = args.length == 0 || args[0].equals("guided");
        in = new Scanner(System.in);
        if (!pause("""
                글 ID만 받는 소비자는 처리할 때마다 원본 API를 부릅니다. 원본이 한 글에서만 실패하거나,
                전부 내려가거나, 느려질 때 소비자에게 무슨 일이 생기는지 다섯 조건에서 확인합니다.
                실제 임시 Kafka와 loopback HTTP를 사용합니다.""")) return;
        try (var kafka = new KafkaRig(); var source = new ContentSource()) {
            headOfLine(kafka, source);
            deadLetterReplay(kafka, source);
            outage(kafka, source);
            slowLookups(kafka, source);
            maxPollInterval(kafka, source);
        } catch (Quit q) {
            System.out.println("종료합니다. 임시 브로커와 HTTP 서버를 정리했습니다.");
            return;
        }
        System.out.println("PASS experiments=5 realBroker=true loopbackHttp=true");
    }

    static FailureConsumer.Handler zero(ContentSource source, ReadModel model) {
        return e -> model.applyLatest(e.contentId(), source.fetch(e.contentId()));
    }

    // 1. 한 글의 조회만 실패한다. 같은 파티션의 다른 글은?
    static void headOfLine(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[1/5] 파티션 0의 첫 글(ID 1)만 조회가 500으로 실패합니다. 파티션 0의 나머지 9건과 파티션 1의 10건은 처리될까요?")) throw new Quit();
        kafka.topic("hol", 2);
        for (long id = 1; id <= 20; id++) kafka.send("hol", id <= 10 ? 0 : 1, Event.zero(source.save(id, "글 " + id)));
        source.fail(1, true);
        var model = new ReadModel();
        var during = FailureConsumer.run(kafka, source, "hol", 2, "hol", zero(source, model), Policy.RETRY_FOREVER, null, Duration.ofSeconds(2));
        long p0Applied = model.views.keySet().stream().filter(id -> id <= 10).count();
        long p1Applied = model.views.keySet().stream().filter(id -> id > 10).count();
        check(during.committed()[0] == 0 && p0Applied == 0, "partition 0 is blocked behind ID 1");
        check(during.committed()[1] == 10 && p1Applied == 10, "partition 1 is not blocked");
        check(during.retries() >= 5, "kept retrying ID 1: " + during.retries());
        source.fail(1, false);
        var after = FailureConsumer.run(kafka, source, "hol", 2, "hol", zero(source, model), Policy.RETRY_FOREVER, null, WAIT);
        check(after.caughtUp() && model.views.size() == 20, "all applied after recovery");
        System.out.printf(Locale.ROOT, "PASS headOfLine during(2s): p0 committed=%d applied=%d/10, p1 committed=%d applied=%d/10, retries=%d | after: p0 committed=%d p1 committed=%d applied=20%n",
                during.committed()[0], p0Applied, during.committed()[1], p1Applied, during.retries(), after.committed()[0], after.committed()[1]);
    }

    // 2. 실패한 이벤트를 DLT로 넘기고 나중에 다시 처리한다. 그 사이 같은 글이 또 바뀌었다.
    static void deadLetterReplay(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[2/5] ID 30의 v1 이벤트가 DLT로 갔고, 그 뒤 v2가 반영됐습니다. DLT의 v1을 재처리하면 zero와 fat 소비자는 각각 어떻게 될까요?")) throw new Quit();
        for (String t : List.of("dlt-zero", "dlt-zero.dlt", "dlt-fat", "dlt-fat.dlt")) kafka.topic(t, 1);
        var v1 = source.save(30, "제목 v1");
        var others = List.of(source.save(31, "글 31"), source.save(32, "글 32"));
        kafka.send("dlt-zero", 0, Event.zero(v1));
        kafka.send("dlt-fat", 0, Event.fat(v1));
        for (var a : others) {
            kafka.send("dlt-zero", 0, Event.zero(a));
            kafka.send("dlt-fat", 0, Event.fat(a));
        }
        // zero: 원본 조회가 ID 30에서 실패한다. fat: 조회는 없지만 자기 저장소 쓰기가 ID 30에서 실패한다고 가정한다.
        source.fail(30, true);
        var fatBroken = new AtomicBoolean(true);
        var zeroModel = new ReadModel();
        var naive = new ReadModel();
        var guarded = new ReadModel();
        FailureConsumer.Handler fatNaive = e -> {
            if (fatBroken.get() && e.contentId() == 30) throw new ContentSource.LookupFailed(500, "store failed id=30");
            naive.applyFat(e);
        };
        FailureConsumer.Handler fatGuarded = e -> {
            if (fatBroken.get() && e.contentId() == 30) throw new ContentSource.LookupFailed(500, "store failed id=30");
            guarded.applyFatGuarded(e);
        };
        var zeroFirst = done(FailureConsumer.run(kafka, source, "dlt-zero", 1, "dlt-zero", zero(source, zeroModel), Policy.RETRY_THEN_DLT, "dlt-zero.dlt", WAIT));
        done(FailureConsumer.run(kafka, source, "dlt-fat", 1, "dlt-fat-naive", fatNaive, Policy.RETRY_THEN_DLT, "dlt-fat.dlt", WAIT));
        done(FailureConsumer.run(kafka, source, "dlt-fat", 1, "dlt-fat-guarded", fatGuarded, Policy.RETRY_THEN_DLT, "dlt-fat.dlt", WAIT));
        check(zeroFirst.deadLettered() == 1 && zeroFirst.retries() == FailureConsumer.MAX_ATTEMPTS - 1 && zeroModel.view(31).isPresent() && zeroModel.view(32).isPresent(),
                "zero moved ID 30 to DLT after 3 attempts and kept going");
        // 원인을 고친 뒤 같은 글이 한 번 더 바뀌고, 그 이벤트는 정상 처리된다.
        source.fail(30, false);
        fatBroken.set(false);
        var v2 = source.save(30, "제목 v2");
        kafka.send("dlt-zero", 0, Event.zero(v2));
        kafka.send("dlt-fat", 0, Event.fat(v2));
        done(FailureConsumer.run(kafka, source, "dlt-zero", 1, "dlt-zero", zero(source, zeroModel), Policy.RETRY_THEN_DLT, "dlt-zero.dlt", WAIT));
        done(FailureConsumer.run(kafka, source, "dlt-fat", 1, "dlt-fat-naive", fatNaive, Policy.RETRY_THEN_DLT, "dlt-fat.dlt", WAIT));
        done(FailureConsumer.run(kafka, source, "dlt-fat", 1, "dlt-fat-guarded", fatGuarded, Policy.RETRY_THEN_DLT, "dlt-fat.dlt", WAIT));
        // 이제 DLT에 남은 v1 이벤트를 재처리한다.
        var zeroDlt = kafka.readAll("dlt-zero.dlt", 1);
        var fatDlt = kafka.readAll("dlt-fat.dlt", 1);
        check(zeroDlt.size() == 1 && zeroDlt.getFirst().version() == 1, "zero DLT holds v1");
        check(fatDlt.size() == 2 && fatDlt.stream().allMatch(e -> e.version() == 1), "fat DLT holds v1 from both fat consumers");
        String beforeReplay = "zero=" + zeroModel.view(30).orElseThrow().title() + ",fatNaive=" + naive.view(30).orElseThrow().title() + ",fatGuarded=" + guarded.view(30).orElseThrow().title();
        source.resetCalls();
        zero(source, zeroModel).handle(zeroDlt.getFirst());
        int replayCalls = source.calls();
        naive.applyFat(fatDlt.getFirst());
        boolean guardedApplied = guarded.applyFatGuarded(fatDlt.getFirst());
        check(zeroModel.view(30).orElseThrow().version() == 2, "zero replay reads v2");
        check(naive.view(30).orElseThrow().version() == 1, "naive fat replay regresses to v1");
        check(!guardedApplied && guarded.view(30).orElseThrow().version() == 2, "guarded fat replay skips v1");
        System.out.println("PASS dlt first: zero dead=" + zeroFirst.deadLettered() + " retries=" + zeroFirst.retries()
                + " others=applied | beforeReplay: " + beforeReplay
                + " | replay v1: zero=" + zeroModel.view(30).orElseThrow().title() + "(apiCalls=" + replayCalls + ")"
                + " fatNaive=" + naive.view(30).orElseThrow().title() + " fatGuarded=" + guarded.view(30).orElseThrow().title() + "(skipped)");
    }

    // 3. 원본 전체가 3초 동안 내려간다. DLT 정책만 있으면?
    static void outage(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[3/5] 원본 전체가 3초 동안 503입니다. '3번 실패하면 DLT' 정책은 이벤트 30건 중 몇 건을 DLT로 보낼까요? /health를 먼저 확인하면요?")) throw new Quit();
        for (String t : List.of("outage-dlt", "outage-dlt.dlt", "outage-health", "outage-health.dlt")) kafka.topic(t, 1);
        for (long id = 40; id < 70; id++) {
            var a = source.save(id, "글 " + id);
            kafka.send("outage-dlt", 0, Event.zero(a));
            kafka.send("outage-health", 0, Event.zero(a));
        }
        var results = new TreeMap<String, FailureConsumer.Result>();
        var timer = Executors.newSingleThreadScheduledExecutor();
        try {
            for (var policy : List.of(Policy.RETRY_THEN_DLT, Policy.HEALTH_GATED)) {
                String topic = policy == Policy.RETRY_THEN_DLT ? "outage-dlt" : "outage-health";
                var model = new ReadModel();
                source.down(true);
                var recovery = timer.schedule(() -> source.down(false), 3000, TimeUnit.MILLISECONDS);
                var result = done(FailureConsumer.run(kafka, source, topic, 1, topic, zero(source, model), policy, topic + ".dlt", WAIT));
                recovery.get();
                check(result.processed()[0] + result.deadLettered() == 30, policy + " accounts for all 30");
                check(model.views.size() == result.processed()[0], policy + " model matches processed");
                results.put(policy.name(), result);
            }
        } finally {
            timer.shutdownNow();
        }
        var dlt = results.get("RETRY_THEN_DLT");
        var health = results.get("HEALTH_GATED");
        int dltTopic = kafka.readAll("outage-dlt.dlt", 1).size();
        check(dlt.deadLettered() >= 1 && dltTopic == dlt.deadLettered(), "outage pushed healthy events to DLT");
        check(health.deadLettered() == 0 && health.processed()[0] == 30 && health.pauses() >= 2, "health gate waited instead");
        System.out.printf(Locale.ROOT, "PASS outage retryThenDlt: dead=%d processed=%d retries=%d | healthGated: dead=%d processed=%d pauses=%d retries=%d%n",
                dlt.deadLettered(), dlt.processed()[0], dlt.retries(), health.deadLettered(), health.processed()[0], health.pauses(), health.retries());
    }

    interface BatchLookup {
        void handle(List<Event> batch) throws Exception;
    }

    record BatchRun(int calls, int batches, double handlerMs) {}

    /** 한 파티션을 끝까지 읽으며 poll 배치마다 lookup을 부른다. 소비자 시작 시간을 빼고 처리 시간만 잰다. */
    static BatchRun drainBatches(KafkaRig kafka, ContentSource source, String topic, String group, BatchLookup lookup) throws Exception {
        var tp = new TopicPartition(topic, 0);
        source.resetCalls();
        int batches = 0;
        long handlerNanos = 0;
        try (var consumer = kafka.consumer(group, 500, Map.of())) {
            consumer.assign(List.of(tp));
            consumer.seek(tp, 0);
            long end = consumer.endOffsets(List.of(tp)).get(tp), committed = 0;
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (committed < end && System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(100)).records(tp);
                if (records.isEmpty()) continue;
                var events = new ArrayList<Event>();
                for (var r : records) events.add(Event.parse(r.value()));
                long t0 = System.nanoTime();
                lookup.handle(events);
                handlerNanos += System.nanoTime() - t0;
                batches++;
                committed = records.getLast().offset() + 1;
                consumer.commitSync(Map.of(tp, new OffsetAndMetadata(committed)));
            }
            check(committed == end, "did not finish " + group);
        }
        return new BatchRun(source.calls(), batches, handlerNanos / 1e6);
    }

    // 4. 원본 응답이 요청마다 20ms 걸린다. 조회 방식에 따라 처리 시간이 얼마나 다를까?
    static void slowLookups(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[4/5] 원본 응답이 요청마다 20ms 걸립니다. 이벤트 200건(글 50개 × 4번 수정)을 처리하는 시간은 조회 방식에 따라 어떻게 다를까요?")) throw new Quit();
        kafka.topic("slow", 1);
        for (int round = 1; round <= 4; round++) {
            for (long id = 100; id < 150; id++) kafka.send("slow", 0, Event.zero(source.save(id, "글 " + id + " 수정 " + round)));
        }
        source.latency(20);
        var models = new TreeMap<String, ReadModel>();
        var runs = new TreeMap<String, BatchRun>();
        var pool = Executors.newFixedThreadPool(8);
        try {
            var each = new ReadModel();
            runs.put("1-each", drainBatches(kafka, source, "slow", "slow-each", batch -> {
                for (var e : batch) each.applyLatest(e.contentId(), source.fetch(e.contentId()));
            }));
            models.put("1-each", each);

            var dedupe = new ReadModel();
            runs.put("2-dedupe", drainBatches(kafka, source, "slow", "slow-dedupe", batch -> {
                for (long id : distinctIds(batch)) dedupe.applyLatest(id, source.fetch(id));
            }));
            models.put("2-dedupe", dedupe);

            var bulk = new ReadModel();
            runs.put("3-bulk", drainBatches(kafka, source, "slow", "slow-bulk", batch -> {
                var ids = distinctIds(batch);
                var found = new java.util.HashMap<Long, ContentSource.Article>();
                for (var a : source.fetchMany(ids)) found.put(a.id(), a);
                for (long id : ids) bulk.applyLatest(id, Optional.ofNullable(found.get(id)));
            }));
            models.put("3-bulk", bulk);

            var parallel = new ReadModel();
            runs.put("4-parallel8", drainBatches(kafka, source, "slow", "slow-parallel", batch -> {
                var futures = new ArrayList<java.util.concurrent.Future<Optional<ContentSource.Article>>>();
                for (var e : batch) futures.add(pool.submit((Callable<Optional<ContentSource.Article>>) () -> source.fetch(e.contentId())));
                // 조회는 동시에 하되 반영은 이벤트 순서대로 한다.
                for (int i = 0; i < batch.size(); i++) {
                    try {
                        parallel.applyLatest(batch.get(i).contentId(), futures.get(i).get());
                    } catch (ExecutionException ex) {
                        throw (Exception) ex.getCause();
                    }
                }
            }));
            models.put("4-parallel8", parallel);
        } finally {
            source.latency(0);
            pool.shutdownNow();
        }
        for (var entry : models.entrySet()) {
            for (long id = 100; id < 150; id++) {
                var v = entry.getValue().view(id).orElseThrow();
                check(v.version() == 4 && v.title().equals("글 " + id + " 수정 4"), entry.getKey() + " final state");
            }
        }
        var each = runs.get("1-each");
        check(each.calls() == 200 && each.handlerMs() >= 200 * 20, "sequential per-event lookups pay 200 x 20ms");
        check(runs.get("2-dedupe").calls() >= 50 && runs.get("2-dedupe").calls() <= 200, "dedupe calls between ids and events");
        check(runs.get("3-bulk").calls() == runs.get("3-bulk").batches(), "bulk is one call per batch");
        check(runs.get("4-parallel8").calls() == 200, "parallel still calls once per event");
        var line = new StringBuilder("PASS slow latency=20ms events=200 contents=50 final=v4");
        for (var entry : runs.entrySet()) {
            var r = entry.getValue();
            line.append(String.format(Locale.ROOT, " | %s calls=%d batches=%d handlerMs=%.0f", entry.getKey(), r.calls(), r.batches(), r.handlerMs()));
        }
        System.out.println(line);
    }

    static List<Long> distinctIds(List<Event> batch) {
        var ids = new LinkedHashSet<Long>();
        for (var e : batch) ids.add(e.contentId());
        return new ArrayList<>(ids);
    }

    record PollRun(int fetches, int commitsOk, int commitsFailed, String failure, long committed, double elapsedMs) {}

    /** 그룹을 구독(subscribe)하고 배치마다 이벤트당 조회 후 커밋한다. */
    static PollRun subscribeAndProcess(KafkaRig kafka, ContentSource source, String topic, String group, int maxPollRecords, Duration timeout) throws Exception {
        var tp = new TopicPartition(topic, 0);
        int fetches = 0, ok = 0, failed = 0;
        String failure = "-";
        var model = new ReadModel();
        long started = System.nanoTime();
        try (var consumer = kafka.consumer(group, maxPollRecords, Map.of(
                "max.poll.interval.ms", MAX_POLL_INTERVAL_MS, "session.timeout.ms", "6000", "heartbeat.interval.ms", "1000"))) {
            consumer.subscribe(List.of(topic));
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            long deadline = started + timeout.toNanos();
            while (System.nanoTime() < deadline && kafka.committed(group, tp) < end) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) continue;
                long next = 0;
                for (var r : records) {
                    var e = Event.parse(r.value());
                    model.applyLatest(e.contentId(), source.fetch(e.contentId()));
                    fetches++;
                    next = r.offset() + 1;
                }
                try {
                    consumer.commitSync(Map.of(tp, new OffsetAndMetadata(next)));
                    ok++;
                } catch (org.apache.kafka.common.KafkaException e) {
                    failed++;
                    failure = e.getClass().getSimpleName();
                }
            }
        }
        return new PollRun(fetches, ok, failed, failure, kafka.committed(group, tp), (System.nanoTime() - started) / 1e6);
    }

    // 5. 느린 조회 때문에 한 배치 처리 시간이 max.poll.interval.ms를 넘는다.
    static void maxPollInterval(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[5/5] max.poll.interval.ms=2초, 조회 30ms. 한 번에 100건을 받아 처리하면 커밋은 성공할까요? 20건씩 받으면요?")) throw new Quit();
        kafka.topic("poll-interval", 1);
        for (long id = 200; id < 300; id++) kafka.send("poll-interval", 0, Event.zero(source.save(id, "글 " + id)));
        source.latency(30);
        PollRun big, small;
        try {
            source.resetCalls();
            big = subscribeAndProcess(kafka, source, "poll-interval", "poll-100", 100, Duration.ofSeconds(9));
            small = subscribeAndProcess(kafka, source, "poll-interval", "poll-20", 20, WAIT);
        } finally {
            source.latency(0);
        }
        // 판정 전에 관측값부터 출력한다. 설정을 바꿔 판정이 실패해도 숫자를 비교할 수 있게 하기 위해서다.
        System.out.printf(Locale.ROOT, "SEEN pollInterval maxPollIntervalMs=%s latency=30ms | maxPollRecords=100: fetches=%d commitsOk=%d commitsFailed=%d(%s) committed=%d in %.0fms | maxPollRecords=20: fetches=%d commitsOk=%d commitsFailed=%d committed=%d%n",
                MAX_POLL_INTERVAL_MS, big.fetches(), big.commitsOk(), big.commitsFailed(), big.failure(), big.committed(), big.elapsedMs(),
                small.fetches(), small.commitsOk(), small.commitsFailed(), small.committed());
        check(big.commitsFailed() >= 1 && big.committed() == 0 && big.fetches() > 100, "100-record batches never commit and re-fetch");
        check(small.commitsFailed() == 0 && small.committed() == 100 && small.fetches() == 100, "20-record batches commit everything once");
        System.out.println("PASS pollInterval batch100=neverCommitted batch20=committedOnce");
    }

    private static FailureConsumer.Result done(FailureConsumer.Result result) {
        check(result.caughtUp(), "consumer did not catch up");
        return result;
    }

    private static void check(boolean ok, String message) {
        if (!ok) throw new IllegalStateException("FAIL " + message);
    }

    private static boolean pause(String message) {
        System.out.println();
        System.out.println(message);
        if (!guided) return true;
        System.out.print("[Enter: 실행 / q: 종료] ");
        if (!in.hasNextLine()) return false;
        return !in.nextLine().trim().equalsIgnoreCase("q");
    }

    private SourceFailureLab() {}
}
