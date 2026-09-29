# Kafka 08 — 재시도와 멱등 전송

상세 글: [Kafka 연재](https://blog.baekchan.com/post/kafka-재시도는-어디까지-중복을-막을까-멱등-전송업무-중복순서)

JDK 21과 Git. 임시 로컬 브로커 2개·컨트롤러 1개, Docker/운영 접속 없음. 최초 의존성 다운로드가 필요하고 힙 상한은 1GiB입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-08-v1
./play-kafka-08
```

기존 저장소의 변경을 보존하고 태그를 fetch하세요. Mac은 `Kafka-08.command`, 자동 검증은 `./play-kafka-08 verify`. Enter로 진행하고 q로 서버 시작 전 종료합니다. 클러스터는 정상 종료 때 정리됩니다. Windows 대응 명령은 `gradlew.bat :kafka:08-idempotence-retries:run --args=guided --console=plain`이며 macOS에서만 실행 검증했습니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증](VERIFICATION.md)
