package playground.events;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;
import java.util.Set;
import org.apache.kafka.common.errors.RecordTooLargeException;
import playground.events.Events.Format;

/**
 * fat event와 zero payload 이벤트를 같은 변경, 같은 브로커, 같은 조회 모델 규칙으로 비교한다.
 * guided: 실험마다 질문을 보여 주고 Enter를 기다린다. verify: 질문 없이 모두 실행하고 판정한다.
 */
public final class ZeroPayloadLab {
    private static final Duration WAIT = Duration.ofSeconds(15);
    private static boolean guided;
    private static Scanner in;

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0]))) {
            throw new IllegalArgumentException("guided|verify");
        }
        guided = args.length == 0 || args[0].equals("guided");
        in = new Scanner(System.in);
        if (!pause("""
                이벤트에 데이터를 모두 싣는 fat event와, 글 ID만 보내고 소비자가 원본 API를 다시 조회하는
                zero payload를 여섯 조건에서 비교합니다. 실제 임시 Kafka와 loopback HTTP를 사용합니다.""")) return;
        try (var kafka = new KafkaRig(); var source = new ContentSource()) {
            ordering(kafka, source);
            size(kafka);
            deletion(kafka, source);
            outage(kafka, source);
            lookupCost(kafka, source);
            replicaLag(kafka, source);
        } catch (Quit q) {
            System.out.println("종료합니다. 임시 브로커와 HTTP 서버를 정리했습니다.");
            return;
        }
        System.out.println("PASS experiments=6 realBroker=true loopbackHttp=true");
    }

    // 1. 같은 글의 v2 이벤트가 v1보다 먼저 도착한다.
    static void ordering(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[1/6] v2 이벤트가 v1보다 먼저 도착하면 각 소비자의 최종 제목은 무엇일까요?")) throw new Quit();
        long id = 1;
        var v1 = source.save(id, Article.PUBLISHED, "제목 A", "본문 A");
        var v2 = source.save(id, Article.PUBLISHED, "제목 B", "본문 B");
        kafka.topic("order-fat");
        kafka.topic("order-zero");
        // 두 릴레이가 경쟁하거나 key가 달라 파티션이 갈린 상황을 흉내 내려고 전송 순서를 직접 뒤집는다.
        for (var a : List.of(v2, v1)) {
            kafka.send("order-fat", id, Events.encode(Format.FAT, a));
            kafka.send("order-zero", id, Events.encode(Format.ZERO, a));
        }
        var naive = new ReadModel("fat-naive");
        var guarded = new ReadModel("fat-guarded");
        var zero = new ReadModel("zero");
        source.resetCalls();
        done(kafka.drain("order-fat", "order-naive", KafkaRig.each(naive.fatNaive()), 500, WAIT));
        done(kafka.drain("order-fat", "order-guarded", KafkaRig.each(guarded.fatGuarded()), 500, WAIT));
        done(kafka.drain("order-zero", "order-zero", KafkaRig.each(zero.zero(source)), 500, WAIT));
        int calls = source.calls();
        check(naive.view(id).orElseThrow().title().equals("제목 A"), "naive should end with the stale v1 title");
        check(guarded.view(id).orElseThrow().title().equals("제목 B") && guarded.skipped == 1, "guard should skip v1");
        check(zero.view(id).orElseThrow().title().equals("제목 B"), "zero should read v2");
        check(zero.fetchedVersions.equals(List.of(2L, 2L)) && calls == 2, "zero should fetch v2 twice");
        System.out.println("PASS order sent=[v2,v1] fatNaive=" + naive.steps + " fatGuarded=" + guarded.steps
                + " zero=" + zero.steps + " zeroFetched=" + zero.fetchedVersions + " apiCalls=" + calls);
    }

    // 2. 본문이 커지면 fat event 자체가 전송되지 않을 수 있다.
    static void size(KafkaRig kafka) throws Exception {
        if (!pause("[2/6] 기본 설정의 Producer로 UTF-8 1.2MB 본문을 fat event에 실으면 어떻게 될까요?")) throw new Quit();
        kafka.topic("size-fat");
        kafka.topic("size-zero");
        var normal = new Article(2, 1, Article.PUBLISHED, "보통 글", "가".repeat(2_000));
        int normalFat = KafkaRig.bytes(Events.encode(Format.FAT, normal));
        int normalZero = KafkaRig.bytes(Events.encode(Format.ZERO, normal));
        var large = new Article(3, 1, Article.PUBLISHED, "긴 글", "가".repeat(400_000));
        String largeFat = Events.encode(Format.FAT, large);
        String largeZero = Events.encode(Format.ZERO, large);
        String fatResult;
        try {
            kafka.send("size-fat", large.id(), largeFat);
            fatResult = "sent";
        } catch (RecordTooLargeException e) {
            fatResult = "RecordTooLargeException";
        }
        var zeroMeta = kafka.send("size-zero", large.id(), largeZero);
        check(fatResult.equals("RecordTooLargeException"), "large fat event should be rejected by producer");
        check(zeroMeta.offset() == 0 && kafka.readAll("size-fat").isEmpty(), "zero sent, fat topic empty");
        System.out.printf(Locale.ROOT,
                "PASS size normalBody=6000B fatEvent=%dB zeroEvent=%dB largeBody=1200000B largeFatEvent=%dB fat=%s zero=sent(%dB) fatTopicRecords=0%n",
                normalFat, normalZero, KafkaRig.bytes(largeFat), fatResult, KafkaRig.bytes(largeZero));
    }

    // 3. 소비자가 늦게 따라오는 동안 글이 비공개 후 삭제됐다.
    static void deletion(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[3/6] 공개→비공개→삭제가 끝난 뒤에야 소비자가 세 이벤트를 처리합니다. 실수로 공개한 문장이 어디에 남을까요?")) throw new Quit();
        long id = 4;
        String marker = "실수로 공개한 연락처 010-0000-0000";
        var changes = List.of(
                source.save(id, Article.PUBLISHED, "공개 글", "본문 " + marker),
                source.save(id, Article.PRIVATE, "공개 글", "본문 " + marker),
                source.delete(id));
        kafka.topic("delete-fat");
        kafka.topic("delete-zero");
        for (var a : changes) {
            kafka.send("delete-fat", id, Events.encode(Format.FAT, a));
            kafka.send("delete-zero", id, Events.encode(Format.ZERO, a));
        }
        var fat = new ReadModel("fat-guarded");
        var zero = new ReadModel("zero");
        source.resetCalls();
        done(kafka.drain("delete-fat", "delete-fat", KafkaRig.each(fat.fatGuarded()), 1, WAIT));
        done(kafka.drain("delete-zero", "delete-zero", KafkaRig.each(zero.zero(source)), 1, WAIT));
        long fatExposed = fat.steps.stream().filter(s -> !s.equals("absent")).count();
        long zeroExposed = zero.steps.stream().filter(s -> !s.equals("absent")).count();
        long fatMarker = kafka.readAll("delete-fat").stream().filter(v -> v.contains(marker)).count();
        long zeroMarker = kafka.readAll("delete-zero").stream().filter(v -> v.contains(marker)).count();
        check(fatExposed == 1 && zeroExposed == 0, "fat shows deleted text once, zero never");
        check(fatMarker == 2 && zeroMarker == 0, "fat topic keeps the text in v1 and v2");
        check(fat.view(id).isEmpty() && zero.view(id).isEmpty(), "both end absent");
        System.out.println("PASS delete fatSteps=" + fat.steps + " zeroSteps=" + zero.steps
                + " fatRecordsWithText=" + fatMarker + " zeroRecordsWithText=" + zeroMarker + " apiCalls=" + source.calls());
    }

    // 4. 원본 조회 API가 503을 돌려준다.
    static void outage(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[4/6] 원본 API가 1.5초 동안 503을 반환합니다. 각 소비자의 커밋 오프셋은 어떻게 될까요?")) throw new Quit();
        long id = 5;
        var a = source.save(id, Article.PUBLISHED, "장애 중 발행한 글", "본문");
        kafka.topic("outage-fat");
        kafka.topic("outage-zero");
        kafka.send("outage-fat", id, Events.encode(Format.FAT, a));
        kafka.send("outage-zero", id, Events.encode(Format.ZERO, a));
        var fat = new ReadModel("fat-guarded");
        var zero = new ReadModel("zero");
        source.resetCalls();
        source.down(true);
        var fatDrain = kafka.drain("outage-fat", "outage-fat", KafkaRig.each(fat.fatGuarded()), 500, WAIT);
        var during = kafka.drain("outage-zero", "outage-zero", KafkaRig.each(zero.zero(source)), 500, Duration.ofMillis(1500));
        int callsDuring = source.calls();
        boolean absentDuring = zero.view(id).isEmpty();
        source.down(false);
        var after = kafka.drain("outage-zero", "outage-zero", KafkaRig.each(zero.zero(source)), 500, WAIT);
        done(fatDrain);
        done(after);
        check(fat.view(id).isPresent(), "fat applies without the API");
        check(during.committed() == 0 && during.handled() == 0 && during.retries() >= 2 && absentDuring, "zero waits during outage");
        check(zero.view(id).isPresent() && after.retries() == 0, "zero applies after recovery");
        System.out.println("PASS outage fat=applied(committed=" + fatDrain.committed() + ") zeroDuring=handled:"
                + during.handled() + ",committed:" + during.committed() + ",retries:" + during.retries()
                + ",http503:" + callsDuring + ",last=" + during.lastRetry()
                + " zeroAfter=applied(committed=" + after.committed() + ")");
    }

    // 5. 글 20개를 10번씩 고친 200개의 이벤트.
    static void lookupCost(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[5/6] 글 20개 × 수정 10번 = 이벤트 200개. 원본 API 호출은 몇 번 일어날까요? poll 단위로 합치면요?")) throw new Quit();
        kafka.topic("cost-fat");
        kafka.topic("cost-zero");
        long fatBytes = 0, zeroBytes = 0;
        for (int round = 1; round <= 10; round++) {
            for (long id = 100; id < 120; id++) {
                var a = source.save(id, Article.PUBLISHED, "글 " + id + " 수정 " + round, "나".repeat(1_000));
                String fat = Events.encode(Format.FAT, a), zero = Events.encode(Format.ZERO, a);
                fatBytes += KafkaRig.bytes(fat);
                zeroBytes += KafkaRig.bytes(zero);
                kafka.send("cost-fat", id, fat);
                kafka.send("cost-zero", id, zero);
            }
        }
        var fat = new ReadModel("fat-guarded");
        var each = new ReadModel("zero-each");
        var merged = new ReadModel("zero-coalesced");
        source.resetCalls();
        long t0 = System.nanoTime();
        var fatDrain = done(kafka.drain("cost-fat", "cost-fat", KafkaRig.each(fat.fatGuarded()), 500, WAIT));
        long t1 = System.nanoTime();
        int fatCalls = source.calls();
        var eachDrain = done(kafka.drain("cost-zero", "cost-each", KafkaRig.each(each.zero(source)), 500, WAIT));
        long t2 = System.nanoTime();
        int eachCalls = source.calls() - fatCalls;
        var mergedDrain = done(kafka.drain("cost-zero", "cost-merged", batch -> {
            try {
                merged.zeroCoalesced(source, batch);
            } catch (ReadModel.RetryLater e) {
                throw new KafkaRig.RetryAt(0, e);
            }
        }, 500, WAIT));
        long t3 = System.nanoTime();
        int mergedCalls = source.calls() - fatCalls - eachCalls;
        for (var model : List.of(fat, each, merged)) {
            for (long id = 100; id < 120; id++) {
                var view = model.view(id).orElseThrow();
                check(view.version() == 10 && view.title().equals("글 " + id + " 수정 10"), model.name + " final state");
            }
        }
        check(fatCalls == 0 && eachCalls == 200, "fat needs no call, zero calls once per event");
        // 합친 뒤의 호출 수는 poll 배치가 어떻게 나뉘었는지에 달려 있어 정확한 값 대신 범위만 판정한다.
        check(mergedCalls >= 20 && mergedCalls <= 200, "coalesced calls between distinct ids and events");
        System.out.printf(Locale.ROOT,
                "PASS cost events=200 contents=20 fatCalls=%d zeroEachCalls=%d zeroCoalescedCalls=%d coalescedBatches=%d fatTopicBytes=%d zeroTopicBytes=%d%n",
                fatCalls, eachCalls, mergedCalls, mergedDrain.batches(), fatBytes, zeroBytes);
        System.out.printf(Locale.ROOT,
                "INFO cost singleRunMs fat=%.1f zeroEach=%.1f zeroCoalesced=%.1f batches fat=%d each=%d (not a benchmark)%n",
                (t1 - t0) / 1e6, (t2 - t1) / 1e6, (t3 - t2) / 1e6, fatDrain.batches(), eachDrain.batches());
    }

    // 6. 이벤트는 커밋 후 나갔지만 조회 API는 아직 이전 상태를 읽는다.
    static void replicaLag(KafkaRig kafka, ContentSource source) throws Exception {
        if (!pause("[6/6] 이벤트가 도착했을 때 조회 API가 아직 v1을 돌려준다면 zero payload 소비자는 무엇을 저장할까요?")) throw new Quit();
        long id = 6;
        source.save(id, Article.PUBLISHED, "제목 v1", "본문");
        source.lagReplica();
        var v2 = source.save(id, Article.PUBLISHED, "제목 v2", "본문");
        kafka.topic("lag-zero");
        kafka.topic("lag-zero-version");
        kafka.send("lag-zero", id, Events.encode(Format.ZERO, v2));
        kafka.send("lag-zero-version", id, Events.encode(Format.ZERO_VERSION, v2));
        var zero = new ReadModel("zero");
        var versioned = new ReadModel("zero-version");
        var zeroDrain = done(kafka.drain("lag-zero", "lag-zero", KafkaRig.each(zero.zero(source)), 500, WAIT));
        var during = kafka.drain("lag-zero-version", "lag-zero-version", KafkaRig.each(versioned.zeroVersion(source)), 500, Duration.ofMillis(1000));
        check(during.committed() == 0 && during.retries() >= 2 && versioned.view(id).isEmpty(), "versioned waits for v2");
        source.syncReplica();
        var after = done(kafka.drain("lag-zero-version", "lag-zero-version", KafkaRig.each(versioned.zeroVersion(source)), 500, WAIT));
        var zeroAgain = kafka.drain("lag-zero", "lag-zero", KafkaRig.each(zero.zero(source)), 500, Duration.ofMillis(500));
        check(zero.view(id).orElseThrow().version() == 1 && zeroDrain.committed() == 1 && zeroAgain.handled() == 0,
                "plain zero stores v1 and has nothing left to correct it");
        check(versioned.view(id).orElseThrow().version() == 2 && after.committed() == 1, "versioned stores v2");
        System.out.println("PASS replica zero=" + zero.steps + "(committed=" + zeroDrain.committed()
                + ", noMoreEvents) zeroVersion=retries:" + during.retries() + ",last=" + during.lastRetry()
                + " then " + versioned.steps + "(committed=" + after.committed() + ")");
    }

    private static KafkaRig.Drain done(KafkaRig.Drain drain) {
        check(drain.committed() == drain.end(), "Drain did not reach end: " + drain);
        return drain;
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

    /** guided 실행 중 q를 누르면 자원을 닫고 끝내기 위한 신호. */
    private static final class Quit extends RuntimeException {}

    private ZeroPayloadLab() {}
}
