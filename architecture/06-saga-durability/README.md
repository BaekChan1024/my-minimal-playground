# Architecture 06 — Saga: 오케스트레이터가 죽거나 Saga가 멈출 때

상세 글: [블로그에서 읽기](https://blog.baekchan.com/post/오케스트레이터가-죽거나-saga가-멈추면-크래시참여자-중단피벗-뒤-응답-유실-실험)

JDK21과 Git이 필요합니다. 실제 임시 Kafka 4.1.1(브로커1·컨트롤러1)과 임시 PostgreSQL 18.4를 실행합니다. Docker나 외부 Kafka·DB는 필요 없습니다. 첫 실행 시 Maven Central에서 의존성(PostgreSQL 바이너리 포함)을 받습니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach saga-05-v1
./play-saga-05
```

Mac에서는 `Saga-05.command`를 더블클릭해도 됩니다. 실험마다 질문이 나오고 Enter로 실행, q로 종료합니다. 자동 검증은 `./play-saga-05 verify`.

[Architecture 03](../03-saga-orchestration-choreography/README.md)의 오케스트레이션 Saga(검수 단계 제외)를 가져왔습니다: 한도 예약 → 글 잠금 → AI 요약 생성 → 요약 게시(피벗) → 사용량 확정. [04](../04-saga-message-faults/README.md)에서 정한 규칙(Inbox, 상태 가드, 예약 행으로 한 번만 잡기, 처음 보는 Saga의 해제에 "취소됨" 표시)은 늘 켜 둡니다.

## 스위치(`Flow.Options`)

| 스위치 | 하는 일 |
|---|---|
| `direct` | 오케스트레이터가 Outbox 대신 DB 커밋 뒤 Kafka로 바로 보낸다 |
| `deadlineMs` | 피벗 전 단계(예약·잠금·생성)를 이 시간까지 기다리고, 넘으면 보상한다(예약 중이면 해제, 잠금·생성 중이면 잠금 해제 → 예약 해제) |
| `lateCharge` | 시한으로 보상한 Saga에 생성 결과가 늦게 오면, 쓴 토큰을 Usage에 늦은 사용량으로 기록한다 |
| `afterPivot` | 사용량 확정 응답이 `retryMs` 안에 오지 않을 때: `WAIT`(기다림) / `RETRY`(같은 명령을 새 메시지 ID로 다시 보냄, 간격 두 배씩) / `COMPENSATE`(요약을 내리고 예약 해제) |
| `naive` | Usage 확정에서 "아직 예약 중일 때만" 조건을 뺀다 |

장애 주입: `Node.crashAfter/crashAt`(N번째 메시지를 처리해 DB에 커밋한 직후 멈춤. direct면 보낼 메시지를 보내지 않고, Outbox면 릴레이도 멈춤. 오프셋은 커밋하지 않음), `Node.pause/resume`(소비 멈춤), `Flow.dropFirstCommitReply`(Usage가 확정은 반영하고 첫 응답만 보내지 않음).

## 실험

| 실험 | 장애 | 비교 |
|---|---|---|
| 1 오케스트레이터 크래시 | Saga 20개 중 10번째 `PostLocked`를 처리해 GENERATING을 커밋한 직후 오케스트레이터가 멈춤 → 같은 DB·같은 소비 그룹으로 재시작 | Outbox / 커밋하고 바로 보내기 |
| 2 참여자 중단 | AI 소비를 멈춘 채 Saga 10개, 1초 뒤 같은 글에 10개 더, 2초 뒤 AI 재개 | 시한 없음 / 0.5초 시한 / 시한 + 늦은 사용량 기록 |
| 3 피벗 뒤 응답 유실 | Saga 5개의 사용량 확정 응답이 처음 한 번 사라짐 | 기다림 / 0.2초 뒤 재전송 / 재전송 + 조건 없는 Usage / 보상 |

완료 판정: 모든 서비스의 미전송 Outbox 0, 모든 소비 그룹의 지연 0, Saga 상태가 1초 동안 변하지 않음. 끝 상태에 닿지 못하고 멈춘 Saga도 그대로 관측합니다.

## 주의

- 서비스는 한 JVM의 스레드입니다. "크래시"는 소비 루프와 릴레이를 멈추고 객체를 버리는 것이며 프로세스 종료가 아닙니다.
- AI 생성은 흉내이고, 시한으로 보상한 Saga의 생성 요청을 AI에 취소하라고 알리는 기능은 없습니다.
- 워크플로 엔진(Temporal 등)은 실행하지 않았습니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
