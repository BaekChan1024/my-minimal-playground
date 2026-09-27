# Kafka 05 — 입력 오프셋과 출력 레코드 함께 커밋하기

상세 설명은 [블로그에서 읽기](https://blog.baekchan.com/post/kafka-입력-오프셋과-출력-메시지를-함께-커밋하기-sendoffsetstotransaction-실습)에서 확인할 수 있습니다. 이 저장소는 실행 코드·문제·해설·검증 기록을 제공합니다.

## 실행

JDK 21과 Git이 필요합니다. 임시 Kafka가 로컬 포트에서 실행됩니다. Docker·DB·운영 브로커는 필요 없습니다. 첫 실행에는 의존성 다운로드가 필요할 수 있습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-05-v1
./play-kafka-05
```

기존 저장소에서는 변경 파일을 보존한 뒤 `git fetch origin --tags`로 태그를 받으세요. Mac은 `Kafka-05.command`로도 실행합니다. 질문에서 Enter는 진행, q는 종료입니다. 질문은 Consumer·트랜잭션을 닫은 단계 사이에만 표시합니다.

```bash
./play-kafka-05 verify
```

Windows 대응 명령: `gradlew.bat :kafka:05-offsets-in-transaction:run --args=guided --console=plain`. 실행 검증은 macOS에서만 했습니다.

## 비교 기준

입력은 `orders` P0 offset 0 한 건입니다. 서로 다른 그룹이 각각 처리합니다.

- 별도 커밋: 출력 후 입력 커밋을 생략하고 Consumer를 다시 열면 출력 2건, 최종 입력 커밋 1.
- 트랜잭션: 출력과 입력 오프셋을 같이 등록하고 abort한 뒤 재처리하면 RC 출력 1건, 최종 입력 커밋 1.
- 두 방식 모두 처리 시도는 2회입니다. RU에서는 트랜잭션 출력도 abort된 것까지 2건입니다.

RC=`read_committed`, RU=`read_uncommitted`. 강제 종료 대신 정상 close/reopen과 명시적인 abort로 비교합니다. 입력은 실제 subscribe/groupMetadata를 사용하며 트랜잭션 경로에서는 Consumer의 별도 오프셋 커밋을 하지 않습니다. DB·외부 API·모든 장애에서의 exactly-once를 검증하는 실습은 아닙니다.

- [문제](EXERCISES.md)
- [해설](ANSWERS.md)
- [검증 기록](VERIFICATION.md)
- [블로그 안내](ARTICLE.md)
