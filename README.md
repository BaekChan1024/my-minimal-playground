# my-minimal-playground

개념과 상세 해설은 **[개발 블로그의 Kafka 연재](https://blog.baekchan.com/category/kafka)**에서 읽을 수 있습니다.

블로그의 설명을 직접 실행하고, 조건을 바꾸며 확인하는 공개 학습 저장소입니다.
글 전문은 블로그에 발행하고, 이 저장소에는 실행 코드·실습 문제·실행 후 해설·검증 기록을 제공합니다.
한 회차의 코드와 글을 완성한 뒤 실습하고, 원리를 설명할 수 있게 되면 다음 회차로 넘어갑니다.

| 회차 | 주제 | 실행 안내 | 글 |
|---|---|---|---|
| Kafka 01 | 로그·오프셋·커밋·재소비 | [실습 README](kafka/01-log-and-offset/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-첫-실습-읽은-메시지는-사라질까-로그오프셋커밋-이해하기) |
| Kafka 02 | 파티션·키·Consumer Group·재할당 | [실습 README](kafka/02-partitions-and-groups/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-consumer를-늘리면-더-빨라질까-파티션키consumer-group-이해하기) |
| Kafka 03 | poll 제한·재전달·뒤늦은 커밋 거절 | [실습 README](kafka/03-poll-timeout-and-commit/README.md) | [Kafka 연재](https://blog.baekchan.com/category/kafka) — 새 글 발행 준비 중 |

Kafka 03은 `./play-kafka-03` 또는 `Kafka-03.command`로 실행합니다. 자동 검증은 `./play-kafka-03 verify`, 고정 버전은 `kafka-03-v1`입니다.

position·seek·commit·ack의 관계는 [보충 글](https://blog.baekchan.com/post/kafka의-positionseekcommitack-읽은-위치와-처리-완료는-어떻게-다를까)에서 이어서 설명합니다.

Kafka 02는 `./play-kafka-02` 또는 Mac의 `Kafka-02.command`로 시작합니다.
`./play-kafka-02 verify`는 7단계 실험을 자동 실행합니다. 고정 버전은 `kafka-02-v1.1`입니다.
기존 `./play`와 `Kafka-01.command`는 Kafka 01을 그대로 실행합니다.

## 가장 쉬운 실행 (Mac)

저장소 폴더에서 **`Kafka-01.command`를 더블클릭**하고 `1`을 선택하세요.
설치된 JDK 21을 자동으로 찾으며, 읽을 개수를 선택한 다음 Enter로 6단계 실습을 진행합니다.
각 단계는 실행 전에 예상할 질문을 보여 줍니다. 중간에 `q`를 입력하면 종료합니다.

| 선택 | 하는 일 |
|---|---|
| 1 (기본) | 설명과 질문을 읽고 Enter로 실험 진행 |
| 2 | 기존 `seed`, `read`, `seek` 명령으로 자유 실습 |
| 3 | 실제 Kafka에서 7개 시나리오 자동 검증 |
| 0 | 실행하지 않고 종료 |

터미널에서는 저장소 루트에서 `./play`만 입력해도 같은 메뉴가 나옵니다.

## 처음 저장소를 받는 경우

준비물: **JDK 21**, Git, 최초 의존성 다운로드를 위한 인터넷 연결.
Gradle Wrapper가 포함되어 있습니다. Docker·별도 Kafka·DB 설치는 필요 없습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-02-v1.1
./play
```

Mac의 더블클릭 실행 도구와 `./play`는 설치된 JDK를 확인하며 자동 설치하거나 시스템 설정을 변경하지 않습니다.
JDK 21이 없으면 설치 안내를 표시합니다. 최초 다운로드 이후에도 실행 준비 시간은 필요합니다.

Windows에서는 JDK 21을 준비한 뒤 `gradlew.bat :kafka:01-log-and-offset:run --args=guided --console=plain`을 실행합니다.
검증 환경은 macOS ARM64 / JDK 21입니다. Windows·Linux에서의 실행 결과는 아직 확인하지 않았습니다.

자유 실습(메뉴 2)을 선택해 `lab>`가 나타나면 [실습 문제](kafka/01-log-and-offset/EXERCISES.md)를 열고 진행하세요.

```text
seed
read study 2 no-commit
status study
quit
```

자동 검증만 실행하려면:

```bash
./play verify
```

각 실행은 임시 데이터와 임의의 로컬 포트를 사용하는 실제 Kafka 브로커를 만듭니다.
정상 종료하면 브로커와 실습 데이터가 정리되며, 다음 실행은 빈 상태로 시작합니다.
공유 서버 접속 정보나 비밀값은 필요 없습니다.

`kafka-01-v1` 태그는 최초 글의 명령어 실습을 그대로 보존합니다. 쉬운 실행 도구는 `kafka-01-v1.1`부터 포함됩니다.
