# Kafka 04 — 트랜잭션과 읽기 가시성

상세 설명은 [블로그에서 읽기](https://blog.baekchan.com/post/kafka-트랜잭션이-롤백하는-것은-어디까지일까-abortreadcommittedlso)에서 확인할 수 있습니다. 이 저장소는 실행 코드·문제·해설·검증 기록을 제공합니다.

## 실행

JDK 21과 Git이 필요합니다. Gradle Wrapper와 임시 Kafka를 사용하며 Docker·외부 DB·운영 브로커는 필요 없습니다. 첫 실행에는 의존성 다운로드와 로컬 포트 접근이 필요합니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-04-v1
./play-kafka-04
```

이미 받은 저장소에서는 변경 파일을 보존한 뒤 `git fetch origin --tags`로 태그를 받으세요. Mac은 `Kafka-04.command`를 실행해도 됩니다. 두 질문에서 Enter로 진행하며 q로 종료합니다. 질문은 트랜잭션이 열려 있지 않은 시점에만 표시합니다.

```bash
./play-kafka-04 verify
```

Windows 명령은 `gradlew.bat :kafka:04-transactions-and-visibility:run --args=guided --console=plain`입니다. Windows/Linux는 실행 검증하지 않았습니다.

## 비교할 결과

- open: RU는 `[0, 1]`, RC는 `[]`; RC의 읽기 끝 경계는 0.
- abort: RU는 `[0, 1]`, RC는 `[1]`; 외부 효과 메모리 모델은 1.
- commit: 같은 key/value를 명시적으로 두 번 전송하면 RC에서도 두 건.

각 단계는 동일 파티션을 offset 0부터 다시 읽습니다. `commitSync`와 `sendOffsetsToTransaction`은 사용하지 않습니다. Consumer Group 재시작 위치나 end-to-end exactly-once를 검증한 실험이 아닙니다. `externalEffects`는 Java 정수이며 실제 DB·결제 API가 아닙니다. 임시 단일 브로커의 RF/min ISR=1은 운영 권장값이 아닙니다.

- [실습 문제](EXERCISES.md)
- [결과 해설](ANSWERS.md)
- [검증 범위](VERIFICATION.md)
- [블로그 안내](ARTICLE.md)
