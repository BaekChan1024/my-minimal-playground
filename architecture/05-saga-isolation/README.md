# Architecture 05 — Saga: 격리가 없을 때 생기는 일

상세 글: [블로그에서 읽기](https://blog.baekchan.com/post/saga에는-격리가-없다-더티-리드갱신-손실오래된-읽기를-재현하고-대응책의-비용을-재다)

JDK21과 Git이 필요합니다. 임시 PostgreSQL 18.4를 실행합니다. Docker나 외부 DB는 필요 없습니다. 첫 실행 시 Maven Central에서 의존성(PostgreSQL 바이너리 포함)을 받습니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach saga-04-v1
./play-saga-04
```

Mac에서는 `Saga-04.command`를 더블클릭해도 됩니다. 실험마다 질문이 나오고 Enter로 실행, q로 종료합니다. 자동 검증은 `./play-saga-04 verify`.

## 왜 Kafka 없이 하나

[Architecture 03](../03-saga-orchestration-choreography/README.md)·[04](../04-saga-message-faults/README.md)는 메시지 전달을 다뤘습니다. 이번 주제는 Saga의 단계가 **따로 커밋된다**는 사실 자체에서 생기는 문제라서, 메시지 전달을 빼고 단계를 직접 호출합니다. 각 단계는 여전히 Usage 또는 Content database에서 자기 트랜잭션으로 커밋되고, 두 database를 한 트랜잭션으로 묶지 않습니다. 단계를 직접 부르면 두 Saga의 단계를 원하는 순서로 섞을 수 있어 같은 이상 현상을 매번 똑같이 재현할 수 있습니다.

## Saga와 대응책 스위치

`SummarySaga`의 기본 순서: 한도 예약 → 글 읽기 → 요약 생성 → 요약 게시 → 검수 → 사용량 확정. 실패하면 쌓인 보상(게시 되돌리기, 작업 중 표시 지우기, 예약 처리)을 거꾸로 실행합니다. 예약은 요약 생성 전에 실패하면 돌려주고, 생성 뒤에 실패하면(토큰을 이미 썼으므로) 사용량으로 확정합니다. 2·3편과 같은 규칙입니다. 2·3편에서는 글 잠금이 늘 켜져 있었지만, 이번에는 기본값에서 끄고 `postLock` 대응책으로 다시 켭니다.

| 스위치 | 대응책(흔히 부르는 이름) | 하는 일 |
|---|---|---|
| `reorder` | 단계 재배치(pessimistic view) | 검수를 게시보다 앞에 둔다 |
| `hidden` | 결과 감추기(semantic lock) | 게시한 요약을 Saga가 끝날 때까지 독자에게 보이지 않게 한다 |
| `postLock` | 작업 중 표시(semantic lock) | 글에 "이 Saga가 작업 중"을 남기고, 그동안 다른 Saga와 글 수정을 받지 않는다 |
| `casUndo` | 내 것일 때만 되돌리기(compare-and-set) | 게시를 되돌릴 때 요약이 아직 이 Saga의 것일 때만 되돌린다 |
| `reread` | 다시 읽기(reread value) | 게시할 때 글 버전이 읽을 때와 같은지 확인하고, 다르면 실패한다 |
| `reserve=false` | 낙관적 한도 확인 | 시작할 때 잔량만 보고 끝날 때 사용량을 더한다(예약하지 않음) |

## 실험

| 실험 | 이상 현상 | 정해 둔 순서 | 비교 |
|---|---|---|---|
| 1 더티 리드 | 되돌려질 요약을 독자가 본다 | A가 게시까지 → 독자가 읽음 → A 검수 탈락 | naive / reorder / hidden |
| 2 갱신 손실 | 보상이 다른 Saga의 결과를 지운다 | 순서 1: A 게시 → B 끝까지 성공 → A 탈락. 순서 2: A 게시 → B 게시 → A 탈락 → B 탈락 | naive / casUndo / postLock |
| 3 오래된 읽기 | 읽은 뒤 바뀐 글에 옛 요약을 붙인다 | A가 생성까지 → 저자가 글 수정 → A 나머지 | naive / reread / postLock |
| 4 한도 | 예약이 남을 막거나, 확인만 하면 한도를 넘는다 | 한도 1,000에 600짜리 A·B. A가 요약 생성 직전까지 → B 끝까지 → A 나머지(A 생성 실패 / A 성공) | reserve / optimistic |

실험 1·2·4는 이어서 같은 조건을 동시에 실행합니다(스레드 32개 풀에서 Saga 하나를 스레드 하나가 끝까지 실행, 단계 사이 0~2ms 휴식). 실패할 Saga는 고정 시드로 고릅니다(실험 1은 정확히 60건, 실험 2는 200건 중 61건이 검수 탈락, 실험 4는 150건 중 38건이 생성 실패). 동시 실행의 수치는 실행마다 달라지며, 대응책이 구조적으로 0을 보장하는 경우(실험 1의 reorder·hidden, 실험 2의 postLock, 실험 4 reserve의 한도 초과)만 판정합니다.

## 주의

- 메시지 전달·서비스 장애는 없습니다. 단계 호출은 한 JVM 안의 메서드 호출입니다.
- AI 생성은 흉내이고 토큰은 요청한 값 그대로 씁니다.
- 동시 실행의 실패 여부는 고정 시드로 정하지만 스레드 실행 순서는 고정되지 않습니다.
- 검수는 흉내라서 게시 앞으로 옮길 수 있었습니다. 실제로 게시가 끝나야만 할 수 있는 검사라면 옮길 수 없습니다.
- 낙관적 한도(`reserve=false`)는 확정할 때 한도를 다시 확인하지 않습니다. AI 토큰은 생성 시점에 이미 쓰이므로, 끝에서 다시 확인해도 비용은 막지 못하기 때문입니다.
- 작업 중 표시가 남은 채 Saga가 멈추는 경우(표시를 누가 언제 지우나)는 다루지 않습니다. 다음 회차의 주제입니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
