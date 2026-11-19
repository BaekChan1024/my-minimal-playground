# 검증 — 2026-11-19

macOS ARM64 / OpenJDK 21.0.6 / Gradle 9.2.1 / Kafka 4.1.1 (KRaft, 브로커1·컨트롤러1) / PostgreSQL 18.4 (zonky embedded-postgres 2.2.2) / HikariCP 7.0.2 / Jackson 2.19.0.
실험마다 새 database(서비스당 하나), 새 토픽(파티션 1개, 복제 계수 1, key는 Saga ID), 새 소비 그룹을 만듭니다. Producer는 `acks=all`, `linger.ms=0`.
소비자는 자동 커밋을 끄고 수동 할당합니다. 메시지 하나를 (Inbox 기록)·업무 변경·Outbox 기록과 함께 한 DB 트랜잭션으로 처리한 뒤 poll 단위로 `commitSync`합니다. 릴레이는 미전송이면서 보낼 시각이 된 Outbox 행을 5ms 간격으로 읽어 동기 전송합니다.
완료 판정: 모든 Saga가 끝 상태 + 모든 서비스의 미전송 Outbox 0 + 모든 소비 그룹의 지연(끝 오프셋 - 커밋 오프셋) 0이 50ms 간격으로 두 번 연속.

## 자동 검증 출력

```text
PASS dup mode=orchestration config="A inbox+guard+rules" messages=120 inboxDropped=20 guarded=0 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=1270 paidTokens=1270 providerCalls=10
PASS dup mode=orchestration config="B guard+rules" messages=120 inboxDropped=0 guarded=20 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=1270 paidTokens=1270 providerCalls=10
PASS dup mode=orchestration config="C rules" messages=250 inboxDropped=0 guarded=0 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=1270 paidTokens=1270 providerCalls=10
PASS dup mode=orchestration config="D none" messages=250 inboxDropped=0 guarded=0 invariantsOk=false
     reserved=-15000 openReservations=0 lockedPosts=0 used=5080 paidTokens=1270 providerCalls=10 PROBLEMS=[quota.reserved=-15000, used=5080!=paidTokens=1270]
PASS dup mode=orchestration config="E inbox only" messages=120 inboxDropped=20 guarded=0 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=1270 paidTokens=1270 providerCalls=10
PASS dup mode=choreography config="A inbox+guard+rules" messages=80 inboxDropped=20 guarded=0 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=1270 paidTokens=1270 providerCalls=10
PASS dup mode=choreography config="B guard+rules" messages=90 inboxDropped=0 guarded=20 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=1270 paidTokens=1270 providerCalls=10
PASS dup mode=choreography config="C rules" messages=150 inboxDropped=0 guarded=0 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=1270 paidTokens=1270 providerCalls=10
PASS dup mode=choreography config="D none" messages=150 inboxDropped=0 guarded=0 invariantsOk=false
     reserved=-15000 openReservations=0 lockedPosts=0 used=5080 paidTokens=1270 providerCalls=10 PROBLEMS=[quota.reserved=-15000, used=5080!=paidTokens=1270]
PASS dup mode=choreography config="E inbox only" messages=80 inboxDropped=20 guarded=0 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=1270 paidTokens=1270 providerCalls=10
PASS redelivery mode=orchestration config="inbox+rules" restarts=1 usageReceived=7 inboxDropped=1 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=360 paidTokens=360 providerCalls=3
PASS redelivery mode=orchestration config="none" restarts=1 usageReceived=7 inboxDropped=0 invariantsOk=false
     reserved=-500 openReservations=0 lockedPosts=0 used=470 paidTokens=360 providerCalls=3 PROBLEMS=[quota.reserved=-500, used=470!=paidTokens=360]
PASS redelivery mode=choreography config="inbox+rules" restarts=1 usageReceived=10 inboxDropped=1 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=360 paidTokens=360 providerCalls=3
PASS redelivery mode=choreography config="none" restarts=1 usageReceived=10 inboxDropped=0 invariantsOk=false
     reserved=-500 openReservations=0 lockedPosts=0 used=470 paidTokens=360 providerCalls=3 PROBLEMS=[quota.reserved=-500, used=470!=paidTokens=360]
PASS cancelFirst mode=orchestration tombstone=false states={COMPENSATED=3} cancelledMarkers=0 invariantsOk=false
     reserved=1500 openReservations=3 lockedPosts=0 used=0 paidTokens=0 providerCalls=0 PROBLEMS=[quota.reserved=1500, openReservations=3]
     sequence=[orchestrator:ReleaseQuota, usage:QuotaReleased, orchestrator:ReserveQuota, usage:QuotaReserved]
PASS cancelFirst mode=orchestration tombstone=true states={COMPENSATED=3} cancelledMarkers=3 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=0 paidTokens=0 providerCalls=0
     sequence=[orchestrator:ReleaseQuota, usage:QuotaReleased, orchestrator:ReserveQuota, usage:QuotaRejected]
PASS cancelFirst mode=choreography tombstone=false states={COMPENSATED=3} cancelledMarkers=0 invariantsOk=false
     reserved=1500 openReservations=3 lockedPosts=0 used=0 paidTokens=0 providerCalls=0 PROBLEMS=[quota.reserved=1500, openReservations=3]
     sequence=[content:SummaryCancelled, usage:QuotaReleased, content:SummaryRequested, usage:QuotaReserved]
PASS cancelFirst mode=choreography tombstone=true states={COMPENSATED=3} cancelledMarkers=3 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=0 paidTokens=0 providerCalls=0
     sequence=[content:SummaryCancelled, usage:QuotaReleased, content:SummaryRequested, usage:QuotaRejected]
PASS lateReply mode=orchestration at1_5s={COMPENSATED=3} final={COMPENSATED=3} summaries=3 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=360 paidTokens=360 providerCalls=3
     sequence=[orchestrator:ReleaseQuota, usage:QuotaReleased, orchestrator:ReserveQuota, usage:QuotaReserved, orchestrator:LockPost, content:PostLocked, orchestrator:Generate, ai:Generated, orchestrator:AttachSummary, content:SummaryAttached, orchestrator:CommitQuota, usage:QuotaCommitted]
PASS lateReply mode=choreography at1_5s={COMPENSATED=3} final={COMPLETED=3} summaries=3 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 used=360 paidTokens=360 providerCalls=3
     sequence=[content:SummaryCancelled, usage:QuotaReleased, content:SummaryRequested, usage:QuotaReserved, content:PostLocked, ai:SummaryGenerated, content:SummaryAttached, usage:QuotaCommitted]
PASS experiments=4 realBroker=true realPostgres=true
```

고친 뒤 `verify` 3회, 이 실습 커밋을 새로 복제한 디렉터리에서 `./play-saga-03 verify` 2회, 안내 실행(모든 Enter) 1회, 모두 종료 코드 0.
`verify` 다섯 번의 `PASS` 줄, 불변식 줄, 메시지 순서 줄이 글자 하나까지 같았습니다(시간 값은 출력하지 않습니다).
안내 실행 중 2번 질문에서 q를 입력하면 자원을 닫고 종료 코드 0으로 끝나는 것을 확인했습니다. 잘못된 인자는 사용법과 종료 코드 2.

## 실험 3·4를 고친 경위

처음에는 예약 요청을 1.5초 늦췄고 전제를 검사하지 않았습니다. 작업 디렉터리의 `verify` 3회는 모두 "취소됨" 표시 3개였는데, 새로 복제한 디렉터리의 1회에서 코레오그래피 "표시 있음" 조합이 표시 1개로 끝났습니다(불변식은 통과). 3개 중 2개는 취소보다 예약이 먼저 Usage에 닿았을 가능성이 있습니다. 원인은 확인하지 못했습니다. 그래서 지연을 3초로 늘리고, Saga마다 취소 메시지가 예약 요청보다 먼저 전송됐는지 검사하는 전제 조건과 표시 개수 판정을 추가했습니다. 아래 기록은 고친 뒤의 실행입니다.

## 판정 기준

| 실험 | 판정 |
|---|---|
| 1 중복 | 모든 조합에서 Saga 10개 COMPLETED, AI 공급자 호출 10회. 불변식은 D만 깨지고 A·B·C·E는 통과 |
| 2 재전달 | 소비자 재시작 1회. 보호 있음: Inbox가 1건 이상 버리고 불변식 통과. 보호 없음: 불변식 깨짐 |
| 3 취소 선착 | 전제: Saga마다 취소(해제) 메시지가 늦춘 예약 요청보다 먼저 전송됨. Saga 3개 모두 COMPENSATED. "취소됨" 표시 없음: 표시 0개·불변식 깨짐, 있음: 표시 3개·불변식 통과 |
| 4 늦은 응답 | 전제는 3과 같음. 1.5초 시점 3개 모두 COMPENSATED, 최종적으로 요약 3건 게시(최종 상태 값은 출력만 하고 판정하지 않음) |

불변식: `quota.reserved` 합 0, 열린 예약(PENDING·RESERVED) 0, 잠긴 글 0, 모든 서비스 Outbox 미전송 0, Usage `used` 합 = AI의 성공한 `job.tokens` 합.

## 바꿔 보기 확인

실험 3의 시한을 5초로 바꾼 실행: 실험 1·2 통과 후 실험 3에서 `FAIL precondition: cancel was not sent before the request`로 멈춤(종료 코드 1). 출력된 순서에는 취소 메시지 없이 예약부터 확정까지 정상 경로만 있었습니다. 확인 후 되돌렸습니다.

## 검증하지 않은 범위

- 서비스는 한 JVM의 스레드입니다. 소비자 재시작은 객체를 버리고 새로 만든 것이고, 프로세스 종료·리밸런스·네트워크 분리는 재현하지 않았습니다.
- 중복과 지연은 릴레이에 일부러 넣은 것입니다. 실제 원인(릴레이가 보낸 뒤 기록 전에 죽음, Producer 재시도, 릴레이 여러 개의 경쟁, 파티션 키 변경)을 재현한 것은 아닙니다.
- 파티션 1개, 소비자 1개입니다. 같은 Saga의 메시지가 다른 파티션으로 갈라지는 경우, 소비자 여러 개가 같은 Saga를 동시에 처리하는 경우는 다루지 않았습니다.
- AI 생성은 Saga ID로 한 번만 공급자를 부르도록 고정했습니다. 공급자 호출 자체가 중복되는 경우는 다루지 않았습니다.
- 시한은 첫 단계에만 겁니다. 다른 단계의 시한, 재시도, 멈춘 Saga 처리, 오케스트레이터 재시작은 다루지 않았습니다.
- "취소됨" 표시는 영구히 남습니다. 표시를 언제 지울지(보존 기간)는 다루지 않았습니다.
