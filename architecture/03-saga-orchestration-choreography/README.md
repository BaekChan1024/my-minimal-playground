# Architecture 03 — Saga: 오케스트레이션과 코레오그래피

상세 글: [블로그에서 읽기](https://blog.baekchan.com/post/saga를-중앙에서-조율할까-이벤트로-이어-갈까-같은-saga를-두-번-구현해-비교한-여섯-가지-실험)

JDK21과 Git이 필요합니다. 실제 임시 Kafka 4.1.1(브로커1·컨트롤러1)과 임시 PostgreSQL 18.4를 실행합니다. Docker나 외부 Kafka·DB는 필요 없습니다. 첫 실행 시 Maven Central에서 의존성(PostgreSQL 바이너리 포함)을 받습니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach saga-02-v1
./play-saga-02
```

Mac에서는 `Saga-02.command`를 더블클릭해도 됩니다. 실험마다 질문이 나오고 Enter로 실행, q로 종료합니다. 자동 검증은 `./play-saga-02 verify`. 설치된 JDK21을 찾아 사용하고 자동 설치하지 않습니다. 임시 브로커와 DB는 종료 시 정리됩니다.

## 다루는 Saga

"AI 요약 게시" 하나를 두 방식으로 연결합니다. 업무 규칙(`Domain.java`)은 두 방식이 같은 코드를 씁니다.

| 순서 | 서비스 | 일 | 실패하면 |
|---|---|---|---|
| 1 | Usage | 저자 토큰 한도 예약 | 한도 초과 → 거절(REJECTED) |
| 2 | Content | 요약을 붙일 글 잠금 | 글 없음·다른 Saga가 잠금 → 1 해제 |
| 3 | AI | 요약 생성(유료 호출 흉내) | 생성 실패 → 2 잠금 해제 → 1 해제 |
| 4 | Content | 요약 게시, 잠금 해제 | (이 실습에서는 실패시키지 않음) |
| 5 | Usage | 실제 토큰으로 확정 | (이 실습에서는 실패시키지 않음) |

실험 6은 3과 4 사이에 Moderation(요약 검수)을 끼워 넣습니다. 검수에서 떨어지면 잠금을 풀고, 이미 쓴 토큰은 해제가 아니라 사용량으로 확정합니다.

## 구성

| 파일 | 역할 |
|---|---|
| `Infra.java` | 임시 Kafka와 임시 PostgreSQL. 서비스마다 database를 따로 만든다 |
| `Node.java` | 서비스 하나의 실행 틀: Transactional Outbox, Outbox 릴레이, Inbox(메시지 ID 중복 제거), 수동 오프셋 커밋 |
| `Msg.java` | 명령·응답·이벤트 공통 메시지(JSON). 메시지 ID와 Saga ID |
| `Domain.java` | Usage·Content·AI·Moderation의 업무 규칙. 같은 Saga ID로 다시 불려도 한 번만 반영 |
| `Orchestration.java` | 오케스트레이터의 Saga 상태 행과 상태 전이표, 명령 토픽 4개 + 응답 토픽 1개 |
| `Choreography.java` | 서비스별 이벤트 토픽과 구독. 요청자(Content)의 요청 상태 행 |
| `World.java` | 실험마다 새 database·토픽, 완료 대기, 서비스 경계를 넘는 불변식 검사 |
| `SagaLab.java` | 여섯 실험과 판정 |

| 실험 | 비교 |
|---|---|
| 1 정상 경로 | 메시지 수와 순서 |
| 2 보상 | 글 잠금 실패, AI 생성 실패에서 되돌리는 순서와 메시지 수 |
| 3 멈춘 Saga | AI 소비를 멈춘 동안 "어디서 멈췄나"를 어느 DB에서 알 수 있나, 재개 후 완료 |
| 4 섞인 부하 | 성공·잠금 실패·생성 실패·한도 거절이 섞인 60개 동시 실행의 불변식과 메시지 수 |
| 5 의존 | 각 서비스가 다른 서비스가 정의한 메시지를 몇 종류 처리하나(실제 구독 등록 기준) |
| 6 단계 추가 | 검수 단계를 넣을 때 구독이 바뀌는 기존 서비스, 검수 거절 시 과금 |

모든 실험 끝에 불변식을 확인합니다: 남은 예약 0, 잠긴 글 0, Outbox 미전송 0, Usage가 확정한 토큰 = AI가 실제로 쓴 토큰.

## 주의

- 서비스는 한 JVM 안의 스레드입니다. 서비스마다 DB와 Kafka 소비 그룹이 따로 있지만 네트워크 분리·프로세스 장애는 없습니다.
- 토픽은 파티션 1개입니다. Saga 사이의 처리 순서, 파티션 재할당, 소비자 여러 개는 다루지 않습니다.
- Outbox·Inbox로 중복과 유실을 막은 상태에서 비교합니다. 중복·순서 역전·보상이 원 요청보다 먼저 도착하는 경우는 다음 회차에서 이 틀을 일부러 깨뜨려 봅니다.
- 시간 값은 loopback·5ms 릴레이 폴링·20ms poll의 영향이 커서 방식 간 속도 비교로 쓸 수 없습니다. 판정하지 않습니다.
- AI 호출은 흉내입니다. 토큰 수는 글 ID로 정해지는 고정값입니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
