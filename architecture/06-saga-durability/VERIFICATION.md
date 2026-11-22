# 검증 — 2026-11-22

macOS ARM64 / OpenJDK 21.0.6 / Gradle 9.2.1 / Kafka 4.1.1 (KRaft, 브로커1·컨트롤러1) / PostgreSQL 18.4 (zonky embedded-postgres 2.2.2) / HikariCP 7.0.2 / Jackson 2.19.0.
실험마다 새 database(서비스당 하나)와 새 토픽(파티션 1개, 키는 Saga ID)을 만듭니다. 소비자는 수동 할당·자동 커밋 끔이고, 메시지 하나를 Inbox·업무 변경·Outbox와 한 트랜잭션으로 처리한 뒤 poll 단위로 오프셋을 커밋합니다.
시한·재전송 검사는 오케스트레이터 DB를 20ms 간격으로 조회합니다. 완료 판정: 미전송 Outbox 0, 모든 소비 그룹 지연 0, Saga 상태가 1초 동안 그대로.

## 자동 검증 출력

```text
PASS crash mode=outbox states={COMPLETED=20} redeliveredDroppedByInbox=3 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 summaries=20 used=2630 paidTokens=2630 providerCalls=20
PASS crash mode=commitThenSend states={COMPLETED=19, GENERATING=1} redeliveredDroppedByInbox=5 invariantsOk=false
     stuck=[orchestrator:ReserveQuota, usage:QuotaReserved, orchestrator:LockPost, content:PostLocked]
     reserved=500 openReservations=1 lockedPosts=1 summaries=19 used=2500 paidTokens=2500 providerCalls=19 PROBLEMS=[quota.reserved=500, openReservations=1, lockedPosts=1]
PASS participantDown variant=noDeadline at2s={COMPENSATED=10, GENERATING=10} lockedAt2s=10 busy=10 final={COMPENSATED=10, COMPLETED=10} wastedGenerations=0 lateCharges=0 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 summaries=10 used=1270 paidTokens=1270 providerCalls=10
PASS participantDown variant=deadline at2s={COMPENSATED=20} lockedAt2s=0 busy=0 final={COMPENSATED=20} wastedGenerations=20 lateCharges=0 invariantsOk=false
     reserved=0 openReservations=0 lockedPosts=0 summaries=0 used=0 paidTokens=2540 providerCalls=20 PROBLEMS=[used=0!=paidTokens=2540]
PASS participantDown variant=deadline+lateCharge at2s={COMPENSATED=20} lockedAt2s=0 busy=0 final={COMPENSATED=20} wastedGenerations=20 lateCharges=20 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 summaries=0 used=2540 paidTokens=2540 providerCalls=20
PASS lostReply variant=wait states={COMMITTING=5} resends=0 committedReservations=5 summaries=5 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 summaries=5 used=650 paidTokens=650 providerCalls=5
PASS lostReply variant=retry states={COMPLETED=5} resends=5 committedReservations=5 summaries=5 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 summaries=5 used=650 paidTokens=650 providerCalls=5
PASS lostReply variant=retry+naiveUsage states={COMPLETED=5} resends=5 committedReservations=5 summaries=5 invariantsOk=false
     reserved=0 openReservations=0 lockedPosts=0 summaries=5 used=1300 paidTokens=650 providerCalls=5 PROBLEMS=[used=1300!=paidTokens=650]
PASS lostReply variant=compensate states={COMPENSATED=5} resends=0 committedReservations=5 summaries=0 invariantsOk=true
     reserved=0 openReservations=0 lockedPosts=0 summaries=0 used=650 paidTokens=650 providerCalls=5
PASS experiments=3 realBroker=true realPostgres=true
```

`verify` 3회, 이 실습 커밋을 새로 복제한 디렉터리에서 `./play-saga-05 verify` 2회, 안내 실행(모든 Enter) 1회, 모두 종료 코드 0.
실행마다 같았던 값: 판정 대상 전부, 상태 분포, 토큰 합계, 멈춘 Saga의 메시지 순서, 재전송 수(5), 늦은 사용량 기록 수(20).
달랐던 값: 실험 1의 `redeliveredDroppedByInbox`(재시작 뒤 다시 받아 Inbox가 버린 메시지 수) 1~6건. 크래시 시점에 같은 poll 묶음에 있던 메시지 수에 따라 달라지며 판정하지 않습니다.
안내 실행 중 2번 질문에서 q를 입력하면 자원을 닫고 종료 코드 0으로 끝나는 것을 확인했습니다. 잘못된 인자는 사용법과 종료 코드 2.

## 판정 기준

| 실험 | 판정 |
|---|---|
| 1 | Outbox: 재시작 뒤 20개 모두 COMPLETED, 불변식 통과. 커밋하고 바로 보내기: COMPLETED 19·GENERATING 1, 멈춘 Saga의 메시지에 `Generate`가 없음, 불변식 깨짐(예약 1·잠금 1) |
| 2 | 시한 없음: 2초 시점 잠긴 글 10·BUSY 10, AI 재개 뒤 앞선 10개 COMPLETED, 불변식 통과. 시한: 2초 시점 20개 모두 COMPENSATED·잠금 0·BUSY 0, AI 재개 뒤 결과를 못 쓴 생성 20, 불변식 깨짐(사용량 ≠ 실제). 시한 + 늦은 기록: 위와 같고 늦은 기록 20, 불변식 통과 |
| 3 | 응답 유실 5건. 기다림: COMMITTING 5, 확정된 예약 5, 불변식 통과. 재전송: COMPLETED 5, 재전송 5 이상, 불변식 통과. 재전송 + 조건 없는 Usage: COMPLETED 5, 불변식 깨짐(사용량 두 배). 보상: COMPENSATED 5, 요약 0, 확정된 예약 5, 불변식 통과 |

불변식: `quota.reserved` 합 0, 열린 예약 0, 잠긴 글 0, Usage `used` 합 = AI의 성공한 `job.tokens` 합.

## 바꿔 보기 확인

실험 2 "deadline" 변형의 시한을 3초로 바꾼 실행: 시한 없음 변형 통과 후 `FAIL deadline releases {COMPENSATED=10, GENERATING=10}`로 멈춤(종료 코드 1). 확인 후 되돌렸습니다.

## 검증하지 않은 범위

- 크래시는 소비 루프와 릴레이를 멈추고 객체를 버린 것이며 프로세스 종료·컨테이너 재시작·리밸런스가 아닙니다. 크래시 지점은 하나(상태 커밋 직후)만 실험했습니다.
- "커밋하고 바로 보내기"의 반대 순서(보내고 커밋하기)에서 생기는 유령 명령은 실험하지 않았습니다.
- 시한으로 보상한 Saga의 생성 요청을 AI에 취소로 알리는 장치, 시한·재전송 간격의 조정, 재전송 횟수 상한과 그 뒤 처리(사람 개입)는 다루지 않았습니다.
- 사용량 확정만 피벗 뒤 단계로 실험했습니다. 요약 게시 응답 유실은 다루지 않았습니다.
- 워크플로 엔진(Temporal 등)은 실행하지 않았습니다.
