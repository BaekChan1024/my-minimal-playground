# Kafka 03 — poll 제한과 커밋 경계

상세 기고문은 [블로그 Kafka 연재](https://blog.baekchan.com/category/kafka)에 발행합니다. 현재 글은 발행 준비 중입니다.

실제 임시 Kafka에서 Consumer A가 레코드를 읽고 효과를 반영한 뒤 poll을 중단합니다. 같은 그룹의 B가 파티션을 인계받으면 재전달 여부와 A의 뒤늦은 커밋 결과를 비교합니다.

## 실행

준비물은 JDK 21과 Git입니다. Gradle Wrapper가 포함되어 있고 최초 실행에는 의존성 다운로드가 필요합니다. Docker나 외부 서버는 사용하지 않습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-03-v1
./play-kafka-03
```

기존 저장소는 변경 파일을 보존하고 `git fetch origin --tags` 후 태그를 선택합니다. Mac에서는 `Kafka-03.command`를 실행해도 됩니다. 두 질문에서 Enter로 진행하며 q로 종료합니다. 안내 입력을 기다리는 동안 실험 중인 Consumer의 poll을 일부러 멈추지 않도록, 각 질문은 실험 시작 전에 둡니다.

```bash
./play-kafka-03 verify
```

Windows 대응 명령은 `gradlew.bat :kafka:03-poll-timeout-and-commit:run --args=guided --console=plain`입니다. 실행 검증은 macOS ARM64/JDK 21에서만 수행했습니다.

## 비교할 결과

| 조건 | B의 재소비 | 효과 카운터 | 최종 커밋 | A의 뒤늦은 커밋 |
|---|---:|---:|---:|---|
| A가 효과만 반영하고 poll 중단 | 1 | 2 | 1 | CommitFailedException |
| A가 효과 반영 후 커밋하고 poll 중단 | 0 | 1 | 1 | CommitFailedException |

두 조건은 다른 그룹을 사용합니다. 레코드는 하나이며 topic/partition/offset/key/value를 대조합니다. 효과 카운터는 메모리 모델입니다. 실제 DB, 결제 API, 분산 트랜잭션을 검증한 것은 아닙니다.

설정: broker/client 4.1.1, spring-kafka-test 4.0.1, 단일 브로커·파티션·복제 계수 1, classic, RangeAssignor, 동적 멤버십, auto commit=false, earliest, max.poll.records=1, max.poll.interval.ms=2000, session.timeout.ms=6000, heartbeat.interval.ms=1000. 2초는 실습용이며 운영 권장값이 아닙니다.

- [예상하고 실행하기](EXERCISES.md)
- [결과 해설](ANSWERS.md)
- [검증 기록과 한계](VERIFICATION.md)
- [실제 실행 코드](src/main/java/playground/kafka/PollTimeoutLab.java)

Consumer별 전용 스레드를 사용합니다. A의 대기는 latch로 제어하고 B 단독 할당·position·커밋을 확인한 뒤 A의 커밋을 시도합니다. Kafka 멤버십과 타임아웃은 실제 브로커·클라이언트가 처리합니다. 그룹 인계 및 읽기 대기에 상한을 두고 종료 시 스레드와 브로커를 닫습니다.
