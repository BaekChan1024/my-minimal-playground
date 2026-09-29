# Kafka 10 — Log Compaction과 tombstone

상세 글: [Kafka 연재](https://blog.baekchan.com/category/kafka) (발행 후 직접 링크 연결)

JDK 21과 Git. 실제 임시 브로커 1개·컨트롤러 1개를 사용합니다. 외부 Kafka/DB/Docker는 필요 없습니다. 최초 의존성 다운로드와 Cleaner 대기에 수십 초 걸릴 수 있습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-10-v1
./play-kafka-10
```

기존 변경은 보존하고 태그를 fetch하세요. Mac `Kafka-10.command`, 자동 `./play-kafka-10 verify`. Enter로 진행, q로 시작 전 종료. 임시 데이터는 종료 시 정리됩니다. 약 600KB 패딩과 roll marker로 세그먼트를 닫고, 업무 key A/B만 비교합니다. tombstone 만료는 검증하지 않습니다. Windows: `gradlew.bat :kafka:10-log-compaction:run --args=guided --console=plain` (Windows 미실행).

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증](VERIFICATION.md)
