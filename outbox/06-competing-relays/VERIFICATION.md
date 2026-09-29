# 검증 기록

2026-09-29 / macOS ARM64 / JDK21. verifyLab 종료 코드 0.

```text
PostgreSQL=18.4 Kafka=4.1.1 overlappingDbConnections=2 scheduling=deterministic
PASS no-lock selectedA=1 selectedB=1 kafka=E1,E1 sentRows=1
PASS global-guard BWhileALocked=empty BAfterACommit=acquired kafka=E1,E2 sentRows=2
PASS row-skip selectedA=1 selectedB=2 sameKey=post-1 kafka=E2,E1 sentRows=2
PASS rollback-release beforeRollback=empty afterRollback=1 kafka=E1 sentRows=1
PASS early-commit selectedA=1 selectedB=1 durableClaim=false kafka=E1,E1 sentRows=1
ALL 5 SCENARIOS PASSED. No throughput or production scaling claim.
```

실제 두 JDBC 연결의 트랜잭션을 동시에 열고, 실행 순서를 명시적으로 교차시킵니다. 각 SQL에는 5초 statement_timeout이 적용됩니다. Kafka는 1브로커/1파티션/RF1이며 한 Producer에서 ACK를 기다려 순서를 고정합니다. 별도 소비자로 레코드 key·value·offset·전체 개수를 확인하고 DB 완료 행 수도 대조합니다.

미검증: 스레드 부하/처리량, 두 OS 프로세스의 네트워크 경쟁, 운영 배포, 프로세스 강제 종료, lease 기반 점유, 다중 브로커 복제, 다른 OS.

저장소 밖 /private/tmp에서 Outbox-06.command 안내 모드도 다섯 PASS와 종료 코드 0을 확인했습니다.
