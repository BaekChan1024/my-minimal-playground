# Architecture 02 — Zero Payload 소비자와 원본 조회 장애

상세 글: 블로그 발행 후 연결합니다. 1편: [Architecture 01](../01-zero-payload/README.md)

JDK21과 Git이 필요합니다. 실제 임시 Kafka 4.1.1(브로커1·컨트롤러1)과 JDK 내장 HTTP 서버(loopback 임의 포트)를 실행합니다. Docker나 외부 Kafka·DB는 필요 없습니다. 첫 실행 시 Maven Central에서 의존성을 받습니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다. 전체 실행에 약 45초 걸립니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach zero-payload-02-v1
./play-zero-payload-02
```

Mac에서는 `ZeroPayload-02.command`를 더블클릭해도 됩니다. 실험마다 질문이 나오고 Enter로 실행, q로 종료합니다. 자동 검증은 `./play-zero-payload-02 verify`. 설치된 JDK21을 찾아 사용하고 자동 설치하지 않습니다.

## 구성

| 파일 | 역할 |
|---|---|
| `ContentSource.java` | 원본 서비스. 메모리 저장소와 실제 HTTP 조회 API(`GET /internal/contents/{id}`, 여러 건 `?ids=1,2,3`, `GET /health`). 특정 글 ID만 500, 전체 503, 응답 지연(서버 sleep)을 켜고 끌 수 있습니다 |
| `Event.java` | zero payload(글 ID·version)와 fat event(제목 포함) |
| `FailureConsumer.java` | 파티션을 수동 할당해 읽는 소비자. 실패 정책 세 가지: 무한 재시도, 3번 실패하면 DLT, `/health`로 원본 장애를 먼저 구분 |
| `SourceFailureLab.java` | 다섯 실험과 판정, 여러 조회 방식, 그룹 구독 소비자 |

| 실험 | 비교 |
|---|---|
| 1 파티션 막힘 | 한 글의 조회 실패가 같은 파티션의 뒤 레코드와 다른 파티션에 주는 영향 |
| 2 DLT 재처리 | DLT에 간 옛 이벤트를 새 변경 뒤에 재처리할 때 zero·fat(덮어쓰기)·fat(version 비교) |
| 3 원본 전체 장애 | 3초 503 동안 "3번 실패하면 DLT"와 "/health 확인 후 멈춤" |
| 4 느린 원본 | 요청당 20ms일 때 이벤트마다 조회·배치 안 중복 제거·여러 건 조회 API·8개 동시 조회 |
| 5 max.poll.interval.ms | 조회 30ms × 배치 100건이 2초 제한을 넘을 때와 배치 20건일 때 |

## 주의

- 원본 저장소는 메모리 맵이고, 지연은 서버 sleep으로 만든 합성 지연입니다. 실제 DB·네트워크 비용이나 부하가 아닙니다.
- 1~3번 소비자는 파티션을 수동 할당(assign)합니다. 그룹 리밸런스는 5번에서만 다룹니다(classic 프로토콜, RangeAssignor).
- 재시도 간격을 100ms 근처로 맞추려고 1~3번 소비자의 `fetch.max.wait.ms`를 100으로 줄였습니다. 기본값 500에서는 재시도마다 약 0.5초가 걸렸습니다(첫 실행에서 관측).
- `/health`는 이 실습에서 원본 전체 장애와 정확히 일치합니다. 실제 서비스의 health check가 장애를 늘 정확히 반영한다는 뜻이 아닙니다.
- 서킷 브레이커 라이브러리, 지수 백오프, 재시도 토픽 단계, 원본 쪽 과부하 보호는 다루지 않습니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
