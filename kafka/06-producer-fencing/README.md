# Kafka 06 — transactional.id와 Producer fencing

상세 글은 [블로그 Kafka 연재](https://blog.baekchan.com/category/kafka)에 발행 준비 중입니다. 이 저장소에는 실행 코드·문제·해설·검증만 둡니다.

## 실행

JDK 21과 Git이 필요합니다. Docker·DB·운영 Kafka 없이 임시 로컬 KRaft 브로커를 실행합니다. 첫 실행에는 의존성 다운로드가 필요할 수 있습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-06-v1
./play-kafka-06
```

기존 저장소에서는 변경 파일을 보존하고 `git fetch origin --tags`로 태그를 받으세요. Mac은 `Kafka-06.command`로도 시작할 수 있습니다. Enter는 진행, q는 종료입니다. 질문은 트랜잭션이 열려 있지 않은 단계 사이에 표시합니다.

```bash
./play-kafka-06 verify
```

Windows 대응 명령은 `gradlew.bat :kafka:06-producer-fencing:run --args=guided --console=plain`입니다. 실행 검증은 macOS에서만 했습니다.

## 비교

- 같은 transactional.id·서로 다른 client.id: 새 Producer의 initTransactions 뒤 이전 Producer의 commit은 ProducerFencedException. 최종 RC=1, RU=2.
- 서로 다른 transactional.id·같은 client.id: 두 Producer의 commit 모두 성공. 동일 key/value지만 RC=2, RU=2.

RC는 read_committed, RU는 read_uncommitted입니다. 한 JVM의 두 Producer 객체를 순서대로 호출합니다. 프로세스 강제 종료·네트워크 장애·Consumer Group·외부 DB는 사용하지 않습니다.

- [문제](EXERCISES.md)
- [해설](ANSWERS.md)
- [검증](VERIFICATION.md)
- [글 안내](ARTICLE.md)
