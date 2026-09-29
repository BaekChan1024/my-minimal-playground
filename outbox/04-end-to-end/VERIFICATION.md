# 검증 기록

2026-09-29 / macOS ARM64 / JDK 21. `./gradlew :outbox:04-end-to-end:verifyLab --write-locks --console=plain` 성공.

```text
PostgreSQL=18.4 Kafka=4.1.1
PASS save-rollback posts=0 outbox=0 sqlState=23502
PASS normal posts=1 outbox=1 sent=true kafka=1 inbox=1 projection=1 committed=1
PASS ack-gap kafka=2 sameEvent=E101 applied=1 inbox=1 projection=1 committed=2
PASS consumer-db-failure beforeRetry=inbox0,projection0,committed0 afterRetry=inbox1,projection1,committed1
PASS consumer-commit-gap rereadOffset=0 duplicate=true kafka=1 inbox=1 projection=1 committed=1
ALL 5 SCENARIOS PASSED. Compare with ANSWERS.md; this does not assess your understanding.
```

실제 PostgreSQL NOT NULL 위반(SQLSTATE 23502), ACK 이후 예외 주입, consumer 정상 종료·재생성을 사용합니다. 각 실험은 최종 행 수와 내용·Kafka 끝 offset·커밋 위치를 assert합니다. 실패가 났다는 사실만으로 PASS하지 않습니다.

Kafka 1 broker/1 controller, topic 1 partition/RF1, auto commit off, manual assign, max.poll.records=1. 복제 내구성·리밸런스·프로세스 강제 종료·네트워크 응답 유실·DLT·부하·운영 배포·다른 OS는 미검증입니다. 전체 실습 자동 통과는 독자의 직접 실습·숙지를 판정하지 않습니다.

저장소 밖 /private/tmp에서 Outbox-04.command에 Enter를 전달한 안내 모드도 다섯 PASS, 종료 코드 0을 확인했다.
