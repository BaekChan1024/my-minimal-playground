# Architecture 01 — Zero Payload 이벤트와 Fat Event 비교

상세 글: 블로그 발행 후 연결합니다.

JDK21과 Git이 필요합니다. 실제 임시 Kafka 4.1.1(브로커1·컨트롤러1)과 JDK 내장 HTTP 서버(loopback 임의 포트)를 실행합니다. Docker나 외부 Kafka·DB는 필요 없습니다. 첫 실행 시 Maven Central에서 의존성을 받습니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach zero-payload-01-v1
./play-zero-payload-01
```

Mac에서는 `ZeroPayload-01.command`를 더블클릭해도 됩니다. 실험마다 질문이 나오고 Enter로 실행, q로 종료합니다. 자동 검증은 `./play-zero-payload-01 verify`. 설치된 JDK21을 찾아 사용하고 자동 설치하지 않습니다. 임시 브로커 데이터는 종료 시 정리됩니다.

## 구성

| 파일 | 역할 |
|---|---|
| `ContentSource.java` | 원본 서비스. 글 상태는 메모리 맵, 조회 API는 실제 HTTP `GET /internal/contents/{id}`. 장애(503)와 "커밋보다 늦은 조회"를 켜고 끌 수 있습니다 |
| `Events.java` | 같은 변경을 FAT(상태 전체), ZERO(ID·종류), ZERO_VERSION(ID·종류·version)으로 직렬화 |
| `ReadModel.java` | 소비자의 조회 모델(공개 글만 보관)과 네 가지 처리 방식: fat 그대로 덮어쓰기, fat + version 비교, zero(매번 조회), zero + version(조회 결과가 오래되면 재시도) |
| `KafkaRig.java` | 임시 클러스터, 동기 전송, 커밋 위치부터 끝까지 처리하는 소비 루프. 처리에 성공한 레코드까지만 커밋하고 재시도할 레코드로 seek합니다 |
| `ZeroPayloadLab.java` | 여섯 실험과 판정 |

| 실험 | 비교 |
|---|---|
| 1 순서 | v2 이벤트가 v1보다 먼저 도착 |
| 2 크기 | 기본 Producer 설정에서 UTF-8 1.2MB 본문 |
| 3 삭제 | 공개→비공개→삭제 후 늦게 처리하는 소비자, 토픽에 남는 본문 |
| 4 장애 | 원본 API 503 동안의 처리·커밋 |
| 5 비용 | 이벤트 200개(글 20개 × 10번)의 조회 횟수, poll 단위 병합 |
| 6 복제 지연 | 조회 API가 이벤트보다 오래된 상태를 돌려줄 때 |

## 주의

- 원본 저장소는 DB가 아니라 메모리 맵입니다. 6번의 "복제 지연"은 실제 DB 복제가 아니라 조회 결과가 늦는 조건만 흉내 냅니다.
- 1번은 두 릴레이 경쟁이나 파티션 분리를 재현하지 않고 전송 순서를 직접 뒤집습니다.
- loopback HTTP·메모리 조회라 5번의 시간 값은 실제 네트워크·DB 비용을 보여 주지 않습니다. 호출 횟수만 판정합니다.
- 재시도는 100ms 간격 무한 재시도입니다. DLT·백오프 상한·서킷 브레이커는 다루지 않습니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
