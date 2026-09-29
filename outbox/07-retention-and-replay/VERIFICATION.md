# 검증 기록

2026-09-29 / macOS ARM64 / JDK21. verifyLab 종료 코드 0.

```text
PostgreSQL=18.4 Kafka=4.1.1 cutoff=fixed-fixture logRemoval=Admin.deleteRecords
PASS outbox-cleanup removedOldSent=1 retainedRecentSent=1 retainedOldPending=1
PASS retained-inbox kafkaOffset=0 replayResult=DUPLICATE total=5 inbox=1
PASS expired-inbox sameKafkaOffset=0 replayResult=APPLIED total=10 duplicateEffectObserved=true
PASS reused-namespace rebuildTotal=0 result=DUPLICATE missingRebuildObserved=true
PASS isolated-rebuild namespace=rebuild-v2 rebuildTotal=5 originalTotal=10 inbox=2
PASS removed-kafka-log logStart=1 logEnd=1 seek0=OffsetOutOfRange inboxStill=2
ALL 6 SCENARIOS PASSED. Fixed dates and explicit log deletion are not timed-retention measurements.
```

삭제 cutoff와 handled_at은 고정 fixture입니다. 실제 PostgreSQL 트랜잭션으로 Inbox와 증가량 반영을 묶습니다. Kafka 한 레코드를 seek0으로 다시 읽고 key/value/offset을 확인합니다. 임시 로컬 토픽에만 Admin.deleteRecords를 호출하여 low watermark=1, 시작/끝 offset=1, OffsetOutOfRange를 검증했습니다. 원본·재구축 테이블 합계와 Inbox 행 수를 assert합니다.

Kafka1브로커/1파티션/RF1. 미검증: 실제 retention 기간 대기, 비동기 세그먼트 삭제 시점, 운영 정리, DLT, 백업/아카이브 복원, 부하, 무중단 재구축, 다른 OS.

저장소 밖 /private/tmp에서 Outbox-07.command 안내 모드도 여섯 PASS와 종료 코드 0을 확인했습니다.
