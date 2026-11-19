package playground.sourcefailure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * 파티션을 수동 할당해 읽는 소비자. 조회가 실패했을 때의 정책만 바꿔 가며 비교한다.
 * 처리에 성공한(또는 DLT로 넘긴) 레코드까지만 커밋하고, 다시 시도할 레코드로 seek한다.
 */
final class FailureConsumer {
    enum Policy {
        /** 성공할 때까지 같은 레코드를 계속 다시 시도한다. */
        RETRY_FOREVER,
        /** 같은 레코드가 MAX_ATTEMPTS번 실패하면 DLT 토픽으로 보내고 다음 레코드로 넘어간다. */
        RETRY_THEN_DLT,
        /** 실패하면 먼저 원본 /health를 본다. 원본 전체가 내려갔으면 시도 횟수를 세지 않고 전체를 잠시 멈춘다. */
        HEALTH_GATED
    }

    interface Handler {
        void handle(Event event) throws ContentSource.LookupFailed;
    }

    static final int MAX_ATTEMPTS = 3;
    static final long BACKOFF_MS = 100;
    static final long PAUSE_MS = 200;

    record Result(int[] processed, int retries, int deadLettered, int pauses, long[] committed, long[] end, long elapsedMs) {
        boolean caughtUp() {
            for (int p = 0; p < end.length; p++) if (committed[p] < end[p]) return false;
            return true;
        }
    }

    private FailureConsumer() {}

    static Result run(KafkaRig kafka, ContentSource source, String topic, int partitions, String group,
                      Handler handler, Policy policy, String dltTopic, Duration timeout) throws Exception {
        var tps = new ArrayList<TopicPartition>();
        for (int p = 0; p < partitions; p++) tps.add(new TopicPartition(topic, p));
        int[] processed = new int[partitions];
        long[] committed = new long[partitions];
        int retries = 0, dead = 0, pauses = 0;
        var attempts = new HashMap<String, Integer>();
        long started = System.nanoTime();
        // fetch.max.wait.ms 기본값(500ms)이면 seek 직후에도 진행 중인 빈 fetch가 끝나야 다음 요청이 나가
        // 재시도 간격이 BACKOFF_MS보다 크게 늘어난다(첫 실행에서 관측). 재시도 간격을 맞추려고 100ms로 줄인다.
        try (var consumer = kafka.consumer(group, 500, Map.of("fetch.max.wait.ms", "100"))) {
            consumer.assign(tps);
            var stored = consumer.committed(Set.copyOf(tps));
            for (var tp : tps) {
                var meta = stored.get(tp);
                committed[tp.partition()] = meta == null ? 0 : meta.offset();
                consumer.seek(tp, committed[tp.partition()]);
            }
            var endOffsets = consumer.endOffsets(tps);
            long[] end = new long[partitions];
            for (var tp : tps) end[tp.partition()] = endOffsets.get(tp);
            long deadline = started + timeout.toNanos();
            while (System.nanoTime() < deadline && !new Result(processed, 0, 0, 0, committed, end, 0).caughtUp()) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) continue;
                boolean backoff = false;
                long pauseMs = 0;
                List<TopicPartition> batch = new ArrayList<>(records.partitions());
                for (int i = 0; i < batch.size() && pauseMs == 0; i++) {
                    var tp = batch.get(i);
                    int p = tp.partition();
                    long next = -1;
                    for (var r : records.records(tp)) {
                        String key = p + ":" + r.offset();
                        try {
                            handler.handle(Event.parse(r.value()));
                            processed[p]++;
                            attempts.remove(key);
                            next = r.offset() + 1;
                            continue;
                        } catch (ContentSource.LookupFailed failure) {
                            if (policy == Policy.HEALTH_GATED && !source.healthy()) {
                                // 원본 전체 장애: 이 레코드의 잘못이 아니므로 시도 횟수를 세지 않는다.
                                pauses++;
                                consumer.seek(tp, r.offset());
                                for (int j = i + 1; j < batch.size(); j++) {
                                    var rest = batch.get(j);
                                    consumer.seek(rest, records.records(rest).getFirst().offset());
                                }
                                pauseMs = PAUSE_MS;
                                break;
                            }
                            int attempt = attempts.merge(key, 1, Integer::sum);
                            if (policy != Policy.RETRY_FOREVER && attempt >= MAX_ATTEMPTS) {
                                kafka.sendRaw(dltTopic, r.key(), r.value());
                                dead++;
                                attempts.remove(key);
                                next = r.offset() + 1;
                                continue;
                            }
                            retries++;
                            consumer.seek(tp, r.offset());
                            backoff = true;
                            break;
                        }
                    }
                    if (next > committed[p]) {
                        consumer.commitSync(Map.of(tp, new OffsetAndMetadata(next)));
                        committed[p] = next;
                    }
                }
                if (pauseMs > 0) {
                    // 멈춘 동안에도 poll은 계속 부른다. 그룹 구독(subscribe) 소비자라면 poll을 멈추는 순간
                    // max.poll.interval.ms가 흐르기 때문이다. pause된 파티션은 레코드를 돌려주지 않는다.
                    consumer.pause(tps);
                    long until = System.nanoTime() + pauseMs * 1_000_000;
                    while (System.nanoTime() < until) {
                        if (!consumer.poll(Duration.ofMillis(50)).isEmpty()) throw new IllegalStateException("paused partition returned records");
                    }
                    consumer.resume(tps);
                } else if (backoff) {
                    Thread.sleep(BACKOFF_MS);
                }
            }
            return new Result(processed, retries, dead, pauses, committed, end, (System.nanoTime() - started) / 1_000_000);
        }
    }
}
