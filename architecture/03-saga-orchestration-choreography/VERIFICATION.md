# 검증 — 2026-11-19

macOS ARM64 / OpenJDK 21.0.6 / Gradle 9.2.1 / Kafka 4.1.1 (KRaft, 브로커1·컨트롤러1) / PostgreSQL 18.4 (zonky embedded-postgres 2.2.2) / HikariCP 7.0.2 / Jackson 2.19.0.
실험마다 새 database(서비스당 하나)와 새 토픽(파티션 1개, 복제 계수 1, key는 Saga ID)을 만듭니다. Producer는 `acks=all`, `linger.ms=0`.
소비자는 자동 커밋을 끄고 수동 할당합니다. 메시지 하나를 Inbox 기록·업무 변경·Outbox 기록과 함께 한 DB 트랜잭션으로 처리한 뒤, poll 단위로 `commitSync`합니다. 릴레이는 서비스마다 하나이고 미전송 Outbox를 5ms 간격으로 읽어 동기 전송합니다.

## 자동 검증 출력

```text
PASS happy mode=orchestration messages=10 sequence=[orchestrator:ReserveQuota, usage:QuotaReserved, orchestrator:LockPost, content:PostLocked, orchestrator:Generate, ai:Generated, orchestrator:AttachSummary, content:SummaryAttached, orchestrator:CommitQuota, usage:QuotaCommitted]
     reserved=0 lockedPosts=0 summaries=1 charged=110 paidTokens=110
PASS happy mode=choreography messages=6 sequence=[content:SummaryRequested, usage:QuotaReserved, content:PostLocked, ai:SummaryGenerated, content:SummaryAttached, usage:QuotaCommitted]
     reserved=0 lockedPosts=0 summaries=1 charged=110 paidTokens=110
PASS compensation mode=orchestration
     lockFailed(6)=[orchestrator:ReserveQuota, usage:QuotaReserved, orchestrator:LockPost, content:PostLockFailed, orchestrator:ReleaseQuota, usage:QuotaReleased]
     generationFailed(10)=[orchestrator:ReserveQuota, usage:QuotaReserved, orchestrator:LockPost, content:PostLocked, orchestrator:Generate, ai:GenerationFailed, orchestrator:UnlockPost, content:PostUnlocked, orchestrator:ReleaseQuota, usage:QuotaReleased]
     reserved=0 lockedPosts=0 summaries=1 charged=110 paidTokens=110
PASS compensation mode=choreography
     lockFailed(4)=[content:SummaryRequested, usage:QuotaReserved, content:PostLockFailed, usage:QuotaReleased]
     generationFailed(6)=[content:SummaryRequested, usage:QuotaReserved, content:PostLocked, ai:GenerationFailed, content:PostUnlocked, usage:QuotaReleased]
     reserved=0 lockedPosts=0 summaries=1 charged=110 paidTokens=110
PASS stuck mode=orchestration views={orchestrator=[GENERATING=5], usage.reservation=[RESERVED=5], ai.job=rows=0} completedAfterResumeMs=105
PASS stuck mode=choreography views={content.request=[LOCKED=5], usage.reservation=[RESERVED=5], ai.job=rows=0} completedAfterResumeMs=435
PASS mixed mode=orchestration sagas=60 states={COMPENSATED=12, COMPLETED=43, REJECTED=5} messages=536 elapsedMs=288 staleReplies=0 inboxDuplicates=0
     reserved=0 lockedPosts=0 summaries=43 charged=5560 paidTokens=5560
PASS mixed mode=choreography sagas=60 states={COMPENSATED=12, COMPLETED=43, REJECTED=5} messages=328 elapsedMs=698 staleReplies=0 inboxDuplicates=0
     reserved=0 lockedPosts=0 summaries=43 charged=5560 paidTokens=5560
PASS coupling mode=orchestration {orchestrator=types=10 from=[ai, content, usage], usage=types=3 from=[orchestrator], content=types=3 from=[orchestrator], ai=types=1 from=[orchestrator]}
     subscriptions={orchestrator={saga.replies=[Generated, GenerationFailed, PostLockFailed, PostLocked, PostUnlocked, QuotaCommitted, QuotaRejected, QuotaReleased, QuotaReserved, SummaryAttached]}, usage={usage.commands=[CommitQuota, ReleaseQuota, ReserveQuota]}, content={content.commands=[AttachSummary, LockPost, UnlockPost]}, ai={ai.commands=[Generate]}}
PASS coupling mode=choreography {usage=types=4 from=[content], content=types=6 from=[ai, usage], ai=types=1 from=[content]}
     subscriptions={usage={content.events=[PostLockFailed, PostUnlocked, SummaryAttached, SummaryRequested]}, content={ai.events=[GenerationFailed, SummaryGenerated], usage.events=[QuotaCommitted, QuotaRejected, QuotaReleased, QuotaReserved]}, ai={content.events=[PostLocked]}}
PASS addStep mode=orchestration changedSubscriptions=[moderation, orchestrator]
     moderation: NEW {moderation.commands=[Moderate]}
     orchestrator: {saga.replies=[Generated, GenerationFailed, PostLockFailed, PostLocked, PostUnlocked, QuotaCommitted, QuotaRejected, QuotaReleased, QuotaReserved, SummaryAttached]} -> {saga.replies=[Approved, Generated, GenerationFailed, PostLockFailed, PostLocked, PostUnlocked, QuotaCommitted, QuotaRejected, QuotaReleased, QuotaReserved, Rejected, SummaryAttached]}
     rejected=[orchestrator:ReserveQuota, usage:QuotaReserved, orchestrator:LockPost, content:PostLocked, orchestrator:Generate, ai:Generated, orchestrator:Moderate, moderation:Rejected, orchestrator:UnlockPost, content:PostUnlocked, orchestrator:CommitQuota, usage:QuotaCommitted]
     reserved=0 lockedPosts=0 summaries=1 charged=230 paidTokens=230
PASS addStep mode=choreography changedSubscriptions=[content, moderation]
     content: {ai.events=[GenerationFailed, SummaryGenerated], usage.events=[QuotaCommitted, QuotaRejected, QuotaReleased, QuotaReserved]} -> {ai.events=[GenerationFailed], moderation.events=[Approved, Rejected], usage.events=[QuotaCommitted, QuotaRejected, QuotaReleased, QuotaReserved]}
     moderation: NEW {ai.events=[SummaryGenerated]}
     rejected=[content:SummaryRequested, usage:QuotaReserved, content:PostLocked, ai:SummaryGenerated, moderation:Rejected, content:PostUnlocked, usage:QuotaCommitted]
     reserved=0 lockedPosts=0 summaries=1 charged=230 paidTokens=230
PASS experiments=6 realBroker=true realPostgres=true
```

`verify` 3회, 안내 실행(모든 Enter) 1회, 이 실습 커밋을 새로 복제한 디렉터리에서 `./play-saga-02 verify` 1회, 모두 종료 코드 0(실험 순서를 정리하기 전의 1회 실행도 판정 결과가 같았습니다).
실행마다 같았던 값: 판정 대상 전부, 메시지 수와 순서, 상태 분포(`COMPLETED=43, COMPENSATED=12, REJECTED=5`), 토큰 합계, 구독 목록.
달랐던 값: 시간 값. 실험 4의 `elapsedMs`는 오케스트레이션 257~356, 코레오그래피 182~698이었고 실험 3의 재개 후 완료 시간도 70~446ms로 흔들렸습니다.
안내 실행 중 2번 질문에서 q를 입력하면 자원을 닫고 종료 코드 0으로 끝나는 것을 확인했습니다. 잘못된 인자는 사용법과 종료 코드 2.

## 판정 기준

| 실험 | 판정 |
|---|---|
| 1 정상 | 두 방식 모두 COMPLETED. 메시지 오케스트레이션 10, 코레오그래피 6. 불변식 통과 |
| 2 보상 | 잠금 실패·생성 실패 모두 COMPENSATED, 성공 1건 COMPLETED. 불변식 통과 |
| 3 멈춤 | 오케스트레이터 조회 결과 `[GENERATING=5]`, 코레오그래피 요청 행 `[LOCKED=5]`, AI `job` 0행. 재개 후 5개 모두 COMPLETED, 불변식 통과 |
| 4 섞인 부하 | 60개 모두 끝 상태, 불변식 통과(메시지 수·상태 분포는 출력만 하고 판정하지 않음) |
| 5 의존 | 실험 1의 구독 등록과 실험 1~4에서 실제로 메시지를 보낸 서비스로 계산해 출력(판정 없음) |
| 6 단계 추가 | 승인 COMPLETED, 거절 COMPENSATED, 확정 2건(거절된 요약의 토큰도 확정), 요약 1건만 게시, 불변식 통과 |

불변식: `quota.reserved` 합 0, 열린 예약 0, 잠긴 글 0, 모든 서비스 Outbox 미전송 0, Usage `used` 합 = 확정 예약의 `actual` 합 = AI의 성공한 `job.tokens` 합.

## 바꿔 보기 확인

Content의 `QuotaCommitted` 구독을 지운 실행: 오케스트레이션 실험 1 통과 후 코레오그래피 실험 1이 60초 뒤 `choreography sagas did not finish: {…=ATTACHED}`로 실패(종료 코드 1). 확인 후 되돌렸습니다.

## 검증하지 않은 범위

- 모든 서비스가 한 JVM의 스레드입니다. 프로세스 장애, 네트워크 분리, 서비스 재배포는 재현하지 않았습니다. 실험 3의 "멈춤"은 소비 루프를 잠시 쉬게 한 것입니다.
- Outbox·Inbox가 정상 동작하는 조건에서만 비교했습니다. 중복 전달, 순서 역전, 보상이 원 요청보다 먼저 도착하는 경우, 결과를 모르는 타임아웃은 다루지 않았습니다.
- Saga 사이의 격리(같은 글을 두 Saga가 동시에 다룰 때)는 판정하지 않았습니다. 잠금 실패의 `BUSY` 경로는 코드에만 있고 실험이 만들지 않습니다.
- 오케스트레이터의 재시작·타임아웃·멈춘 Saga 자동 처리는 없습니다. 실험 3은 사람이 조회하는 경우만 비교합니다.
- 파티션 1개, 소비자 1개입니다. 처리량·지연은 측정하지 않았고 시간 값은 판정하지 않습니다.
- 실험 5의 의존 수는 구독 등록 기준입니다. 처리기 안의 분기(예: Usage의 `PostUnlocked` 사유 분기)는 세지 않습니다.
