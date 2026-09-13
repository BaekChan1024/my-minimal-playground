# my-minimal-playground

블로그의 설명을 직접 실행하고, 조건을 바꾸며 확인하는 공개 학습 저장소입니다.
한 회차의 코드와 글을 완성한 뒤 실습하고, 원리를 설명할 수 있게 되면 다음 회차로 넘어갑니다.

| 회차 | 주제 | 실행 안내 | 글 |
|---|---|---|---|
| Kafka 01 | 로그·오프셋·커밋·재소비 | [실습 README](kafka/01-log-and-offset/README.md) | [상세 해설](kafka/01-log-and-offset/ARTICLE.md) |

## 첫 실행

준비물: **JDK 21**, Git, 최초 의존성 다운로드를 위한 인터넷 연결.
Gradle Wrapper가 포함되어 있습니다. Docker·별도 Kafka·DB 설치는 필요 없습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git checkout kafka-01-v1
./gradlew :kafka:01-log-and-offset:run --console=plain
```

Windows에서는 `gradlew.bat`를 사용합니다. 검증 환경은 macOS ARM64 / JDK 21입니다.
Windows·Linux에서의 실행 결과는 아직 확인하지 않았습니다.

`lab>`가 나타나면 [실습 문제](kafka/01-log-and-offset/EXERCISES.md)를 열고 진행하세요.

```text
seed
read study 2 no-commit
status study
quit
```

자동 검증만 실행하려면:

```bash
./gradlew :kafka:01-log-and-offset:verifyLab --console=plain
```

각 실행은 임시 데이터와 임의의 로컬 포트를 사용하는 실제 Kafka 브로커를 만듭니다.
정상 종료하면 브로커와 실습 데이터가 정리되며, 다음 실행은 빈 상태로 시작합니다.
공유 서버 접속 정보나 비밀값은 필요 없습니다.
