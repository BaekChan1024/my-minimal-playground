# Kafka 07 — 복제·ISR·acks

상세 글: [Kafka 연재](https://blog.baekchan.com/category/kafka) (발행 후 직접 링크 연결)

JDK 21과 Git이 필요합니다. Docker나 외부 Kafka 없이 임시 브로커 3개와 별도 컨트롤러 1개를 실행합니다. 힙 상한 1GiB 외에도 JVM 메모리가 필요합니다. 처음에는 의존성을 다운로드합니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-07-v1
./play-kafka-07
```

기존 저장소는 변경을 보존하고 태그를 fetch하세요. Mac은 `Kafka-07.command`, 자동 검증은 `./play-kafka-07 verify`입니다. Enter로 시작하고 q는 서버 시작 전 종료합니다. 실행 중 자동으로 로컬 브로커 두 개를 정상 종료·재시작하고 리더를 교체합니다. 종료 시 임시 클러스터를 정리합니다. Windows는 `gradlew.bat :kafka:07-replication-isr:run --args=guided --console=plain`이며 macOS에서만 검증했습니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증](VERIFICATION.md) · [글](ARTICLE.md)
