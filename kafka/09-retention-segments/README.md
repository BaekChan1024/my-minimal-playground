# Kafka 09 — 로그 세그먼트와 retention

상세 글: [Kafka 연재](https://blog.baekchan.com/category/kafka) (발행 후 직접 링크 연결)

JDK 21과 Git. 로컬 임시 브로커 1개·컨트롤러 1개이며 외부 Kafka/DB/Docker가 필요 없습니다. 최초 의존성 다운로드와 백그라운드 정리 대기로 수십 초 걸립니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-09-v1
./play-kafka-09
```

기존 저장소 변경을 보존하고 태그를 fetch하세요. Mac `Kafka-09.command`, 자동 `./play-kafka-09 verify`. Enter로 시작, q로 서버 시작 전 종료. 임시 데이터는 종료 때 정리됩니다. 오래된 timestamp를 명시한 합성 레코드 약 5.4MB를 사용하며 시스템 시계를 바꾸지 않습니다. Windows 대응: `gradlew.bat :kafka:09-retention-segments:run --args=guided --console=plain`; macOS만 실행 검증.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증](VERIFICATION.md)
