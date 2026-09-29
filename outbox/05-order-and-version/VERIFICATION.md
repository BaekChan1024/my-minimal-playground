# 검증 기록

2026-09-29 / macOS ARM64 / JDK21 / PostgreSQL18.4. verifyLab 종료 코드 0.

```text
PostgreSQL=18.4 Kafka=not-started eventOrder=explicit-calls
PASS inbox-only delivery=E2,E1 final=1:old:false inbox=2 regressionObserved=true
PASS version-guard delivery=E2,E1 final=2:new:false inbox=2 oldResult=STALE
PASS same-event replay=E2 result=DUPLICATE final=2:new:false inbox=2
PASS delete-tombstone final=3:<null>:true visible=0 lateOldResult=STALE inbox=4
PASS hard-delete-counterexample final=2:new:false visible=1 resurrectionObserved=true
PASS delta-counterexample deltaV2=7 deltaV1=3 guardedTotal=7 intendedTotal=10 lostDeltaObserved=true
ALL 6 SCENARIOS PASSED. Counterexamples intentionally demonstrate incorrect outcomes.
```

실제 DB의 제목·버전·삭제 표시·Inbox 행 수·공개 행 수·증가량을 assert합니다. 역순은 직접 함수 호출로 주입합니다. Kafka 순서 역전이나 생산자의 버전 발급을 실행한 결과가 아닙니다. 세 개의 반례가 PASS한 것은 의도한 잘못된 결과를 재현했다는 뜻입니다.

미검증: 동시 소비자, 실제 작성/삭제 API, Kafka, 부하, 다른 OS/DB, 스키마 진화, eventId payload 충돌 탐지.

저장소 밖 /private/tmp에서 Outbox-05.command 안내 모드도 여섯 PASS와 종료 코드 0을 확인했습니다.
