# Kafka 11 — batching·linger·gzip

상세 글: [Kafka 연재](https://blog.baekchan.com/post/kafka-전송은-무엇을-기다릴까-batchlinger압축과-지연-측정)

JDK21과 Git. 실제 임시 브로커1·컨트롤러1을 실행하며 별도 Kafka/DB/Docker는 필요 없습니다. 순차 linger=20 조건 때문에 수십 초 이상 걸립니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-11-v1
./play-kafka-11
```

기존 변경을 보존하고 태그를 fetch하세요. Mac `Kafka-11.command`, 자동 `./play-kafka-11 verify`. Enter로 시작, q로 시작 전 종료. 종료 시 임시 데이터 정리. Windows: `gradlew.bat :kafka:11-batching-compression:run --args=guided --console=plain` (Windows 미실행).

5조건×3회, 각40건 워밍업+600건 측정. 데이터는 약1.1KB 반복 문자열. 모든 key/value/offset 순서를 검증합니다. CSV 수치는 환경마다 달라지며 속도 순위를 PASS 조건으로 사용하지 않습니다. 지연은 send 직전→callback, 처리량은 600건의 전체 완료 시간으로 계산합니다. Producer 지표는 워밍업 포함640건입니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 및 CSV](VERIFICATION.md)
