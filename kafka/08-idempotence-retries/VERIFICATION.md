# 검증 — 2026-09-29

macOS ARM64 / JDK21 / Gradle9.2.1 / Kafka4.1.1 / metadata4.1-IV1. 자동 verify와 저장소 밖 Mac 안내 실행 모두 종료 0. 실제 ISR 부족 → retry metric 증가와 Future 미완료 → 복구 후 A,B → 새 send 후 A,B,A.

```text
PASS pending retryObserved=true futureDone=false ISR=1
PASS recovered internalRetry=true records=A,B
PASS explicitResend sameKey=true idempotence=true records=A,B,A
PASS incompatibleConfig maxInFlight=6 rejected=ConfigException
```

응답 유실, 순서 역전, 멱등 off 대조군, DB/Inbox, 성능, 강제 종료는 미검증. 오류 로그는 의도한 ISR 부족이다.
