# Architecture 04 — Saga: 메시지 장애(중복·재전달·순서 역전·늦은 응답)

상세 글: 블로그 발행 후 링크를 추가합니다.

JDK21과 Git이 필요합니다. 실제 임시 Kafka 4.1.1(브로커1·컨트롤러1)과 임시 PostgreSQL 18.4를 실행합니다. Docker나 외부 Kafka·DB는 필요 없습니다. 첫 실행 시 Maven Central에서 의존성(PostgreSQL 바이너리 포함)을 받습니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach saga-03-v1
./play-saga-03
```

Mac에서는 `Saga-03.command`를 더블클릭해도 됩니다. 실험마다 질문이 나오고 Enter로 실행, q로 종료합니다. 자동 검증은 `./play-saga-03 verify`(약 1분). 설치된 JDK21을 찾아 사용하고 자동 설치하지 않습니다.

[Architecture 03](../03-saga-orchestration-choreography/README.md)의 "AI 요약 게시" Saga(검수 단계 제외)를 그대로 가져와 장애를 넣습니다.

## 장애와 방어 스위치

| 스위치 | 위치 | 하는 일 |
|---|---|---|
| `Node.duplicate` | 릴레이 | 지정한 종류의 메시지를 Kafka에 두 번 보낸다(보낸 뒤 "보냈음"을 기록하기 전에 릴레이가 죽고 다시 보낸 경우를 흉내) |
| `Node.crashAfter` | 소비자 | 지정한 종류를 처음 처리하고 DB 커밋까지 끝낸 직후, 오프셋을 커밋하지 않고 소비자를 버린 뒤 새 소비자로 다시 시작. Kafka가 같은 메시지를 실제로 다시 준다 |
| `Node.delay` | Outbox | 지정한 종류를 Outbox에 쓴 뒤 정해진 시간 동안 보내지 않는다. 뒤에 쓴 메시지가 먼저 나간다 |
| `Node.inbox` | 소비자 | 받은 메시지 ID 기록(Inbox) 켜기/끄기 |
| `Options.guard` | 오케스트레이터 / Content | 기대한 Saga 상태일 때만 처리하기 켜기/끄기 |
| `Options.naive` | Usage | 해제·확정에서 "아직 예약 중일 때만" 조건을 뺀다 |
| `Options.tombstone` | Usage | 처음 보는 Saga의 해제 요청에 "취소됨" 표시를 남긴다 |
| `Options.deadlineMs` | 오케스트레이터 / Content | 한도 예약 응답을 이 시간까지 기다리고, 넘으면 Saga를 취소한다 |

## 실험

| 실험 | 장애 | 비교 |
|---|---|---|
| 1 중복 | `QuotaReserved`, `SummaryAttached`를 두 번씩 전송, Saga 10개 | Inbox·상태 가드·업무 규칙 조건 조합 다섯 가지(A~E)의 불변식과 메시지 수 |
| 2 재전달 | Usage가 사용량 확정을 처리한 직후 오프셋 커밋 전에 소비자 재시작, Saga 3개 | 보호 있음(Inbox+조건)과 없음 |
| 3 취소 선착 | 첫 예약 메시지 3초 지연, 시한 0.3초, Saga 3개 | "취소됨" 표시 없음/있음 |
| 4 늦은 응답 | 3과 같은 조건에서 표시와 상태 가드 모두 끔 | 1.5초 시점 상태와 최종 상태, 게시된 요약 |

모든 실험은 Saga가 끝 상태에 닿고, 보내지 않은 Outbox가 없고, 모든 소비 그룹이 끝까지 커밋한 상태가 두 번 연속 관측될 때까지 기다린 뒤 불변식을 봅니다: 남은 예약 0, 열린 예약 0, 잠긴 글 0, Usage 사용량 = AI가 실제로 쓴 토큰. 실험 1의 D, 실험 2의 "보호 없음", 실험 3의 "표시 없음"은 불변식이 깨지는 것이 기대 결과입니다.

## 주의

- 서비스는 한 JVM 안의 스레드입니다. 실험 2의 "죽음"은 소비자 객체를 버리고 새로 만드는 것이고 프로세스 종료가 아닙니다.
- 실험 1의 중복은 릴레이가 일부러 두 번 보내는 것이고, 실험 3·4의 순서 역전은 Outbox 전송을 일부러 늦춘 것입니다. 원인(릴레이가 보낸 뒤 기록 전에 죽음, 릴레이 여러 개의 경쟁, 파티션 키)을 재현한 것이 아닙니다.
- 토픽은 파티션 1개, 소비자 1개입니다. AI 생성은 항상 Saga ID로 한 번만 호출되도록 두었습니다(중복이 와도 저장된 결과를 돌려줌).
- 시한은 첫 단계(한도 예약)에만 겁니다. 다른 단계의 시한·재시도·멈춘 Saga 처리는 다음 회차에서 다룹니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
