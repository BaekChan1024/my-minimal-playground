# Kafka 첫 실습: 읽은 메시지는 사라질까? 로그·오프셋·커밋 이해하기

Kafka로 글 발행 이벤트를 전달한다고 생각해 보자. 조회 서비스가 이벤트를 읽었다. 그런데 서비스를 다시 실행하자 같은 이벤트가 또 들어온다. 반대로 어떤 경우에는 토픽에 이벤트가 남아 있는데도 아무것도 읽지 못한다. 두 현상 모두 오프셋을 어떻게 관리했는지 살펴보면 설명할 수 있다.

이번 글은 Kafka를 처음부터 깊게 이해하기 위한 첫 실습이다. 주제는 세 가지다. 레코드가 저장된 위치, Consumer가 현재 읽고 있는 위치, 재시작을 위해 저장해 둔 위치를 구분한다. 세 위치가 어떻게 달라지는지 실제 Kafka 브로커에 A, B, C를 보내 확인한다.

JDK 21만 있으면 실행할 수 있는 [공개 실습 코드](https://github.com/BaekChan1024/my-minimal-playground/tree/kafka-01-v1/kafka/01-log-and-offset)를 함께 제공한다. 글의 결과를 읽기 전에 직접 예상하고 싶다면 저장소의 EXERCISES.md를 먼저 열어도 좋다. 이번 예제의 버전은 `kafka-01-v1` 태그로 고정했다.

## 1. 먼저 레코드가 남는 곳과 읽는 쪽을 나누자

이벤트를 보내는 프로그램을 Producer, 저장하고 읽기 요청에 응답하는 서버를 Broker, 이벤트를 읽는 프로그램을 Consumer라고 부른다. Topic은 이벤트를 구분하는 이름이고, 실제 로그는 Topic 안의 Partition으로 나뉜다. 이번에는 `playground-offsets`라는 Topic에 Partition 하나만 만든다.

Partition을 이해할 때는 순서대로 레코드를 붙이는 로그를 떠올리면 된다. 여기서 로그는 애플리케이션이 남기는 디버깅 문장이 아니라, 이벤트 레코드가 순서와 위치를 갖고 저장되는 구조다. Kafka는 이러한 저장 구조와 소비자의 읽기 위치를 분리한다. 그 결과 같은 기록을 여러 소비자가 각자의 속도로 읽는 모델을 만들 수 있다. [Kafka 설계 문서](https://kafka.apache.org/41/design/design/)

아래 그림은 이번 실습에서 만들 구조다.

```text
Producer
   │ A, B, C 전송
   ▼
Topic: playground-offsets / Partition: 0
┌────────────┬────────────┬────────────┐
│ offset 0   │ offset 1   │ offset 2   │  다음 위치: 3
│ value A    │ value B    │ value C    │
└────────────┴────────────┴────────────┘
        ▲                    ▲
        │                    │
   group study          group audit
   각자의 저장 위치로 같은 로그를 읽음
```

offset은 파티션 안에서 레코드의 위치를 나타내는 숫자다. 서로 다른 파티션의 offset 10은 서로 다른 위치다. 따라서 레코드를 가리킬 때는 토픽 이름, 파티션 번호, 오프셋을 함께 보아야 한다. 여러 파티션의 오프셋 숫자만 비교해서 이벤트 전체의 시간 순서를 판단할 수는 없다.

## 2. 숫자 세 가지를 구분하면 결과가 읽힌다

| 이름 | 이번 예제에서 뜻하는 것 | A와 B를 읽고 커밋하지 않았다면 |
|---|---|---|
| Record offset | 개별 레코드가 파티션 안에서 차지하는 위치 | A=0, B=1 |
| Consumer position | 현재 Consumer의 다음 읽기 위치 | 2 |
| Committed offset | 그룹에 저장한 다음 시작 위치 | 아직 없음 |

`position`은 현재 Consumer가 얼마나 진행했는지를 나타낸다. `poll()`이 레코드를 반환하면서 진행할 수 있으므로, 애플리케이션의 DB 저장이 완료됐다는 증명은 아니다. `committed offset`은 다른 Consumer를 만들거나 할당을 다시 받았을 때 이어 읽는 기준이 된다. 이번 실습에서는 자동 커밋을 끄고 두 값을 일부러 분리한다. [KafkaConsumer API의 Offsets and Consumer Position](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

커밋하는 숫자는 마지막으로 처리한 레코드의 offset 자체가 아니라 다음에 읽을 위치다. 이 예제에서 B의 offset 1까지 처리했으면 2를 저장한다. `committed=2`는 “offset 2도 처리했다”는 뜻이 아니다.

또 하나의 숫자로 로그의 끝을 보겠다. A, B, C가 0, 1, 2에 저장되면 끝 위치는 3이다. 실습 출력에서는 이를 `end=3`으로 표시한다. 이 예제의 읽을 수 있는 범위는 `[0, 3)`, 즉 시작은 포함하고 끝은 제외하는 구간이다. 이번에는 트랜잭션을 사용하지 않는 단일 브로커이므로 이 단순한 경계로 충분하다. 복제·트랜잭션 환경에서의 High Watermark와 Last Stable Offset은 별도로 구분해야 한다.

## 3. 무엇을 설치하고 어떤 환경에서 실행하는가

실습 프로그램은 Spring의 `EmbeddedKafkaKraftBroker`를 사용해 실제 로컬 Kafka 브로커를 시작한다. 가짜 응답을 반환하는 Mock이 아니다. Producer와 Consumer는 일반 Kafka Java 클라이언트이며 브로커와 통신한다. Spring Boot 웹 서버나 데이터베이스는 띄우지 않는다. [Spring Kafka 테스트 도구 문서](https://docs.spring.io/spring-kafka/reference/testing.html)

| 항목 | 이번 실행 조건 |
|---|---|
| 검증 환경 | macOS ARM64, JDK 21.0.6 |
| 빌드 | Gradle Wrapper 9.2.1 |
| Kafka 라이브러리 | 브로커·클라이언트 4.1.1 |
| 브로커 시작 도구 | spring-kafka-test 4.0.1 |
| 토픽 | 파티션 1개, 복제 계수 1 |
| 데이터 | 동일 키 post-42, 값 A·B·C |
| Consumer | enable.auto.commit=false, auto.offset.reset=earliest |
| poll 상한 | max.poll.records=1 |

최신 버전을 비교하는 실험이 아니라, 고정한 버전의 동작을 반복해 보는 실험이다. 전이 의존성까지 Gradle lockfile에 기록했다. Windows와 Linux에서 실행하는 절차도 유사하지만 이번에 실제 검증한 운영체제는 macOS다.

저장소를 받고 루트에서 실행한다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git checkout kafka-01-v1
java -version
./gradlew :kafka:01-log-and-offset:run --console=plain
```

JDK 21을 설치하고 `JAVA_HOME`을 맞춰 둔다. Windows에서는 `./gradlew` 대신 `gradlew.bat`를 사용한다. 최초 실행에는 의존성을 받기 위한 인터넷 연결이 필요하다. Docker나 별도 Kafka 설치는 필요 없다.

`lab>`가 나타나면 아래 명령을 차례로 입력할 수 있다. **read와 seek 명령마다 Consumer를 새로 만들고 닫는다. 브로커는 quit까지 계속 실행된다.** 따라서 명령 사이에는 커밋과 레코드가 유지되지만, 프로그램 전체를 종료한 뒤 다시 실행하면 새 실습 환경이 된다.

이 회차에서는 `assign()`으로 파티션을 직접 지정했다. 그룹 ID별 오프셋 저장을 사용하되, `subscribe()`로 그룹에 가입하여 파티션을 나누는 리밸런스는 다루지 않는다. 첫 실습의 관찰 대상을 읽기 위치에 집중하기 위한 선택이다. `assign`을 사용한다고 그룹의 자동 작업 분배까지 검증했다고 해석하면 안 된다.

## 4. 실험 1 — 세 레코드를 저장하고 끝 위치를 확인한다

```text
seed
status study
```

Producer는 각 전송의 완료 응답을 기다린 뒤 다음 값을 보낸다. 실제 실행에서 다음 오프셋을 확인했다.

```text
stored value=A partition=0 offset=0
stored value=B partition=0 offset=1
stored value=C partition=0 offset=2
```

`seed`는 빈 토픽에서 한 번만 동작한다. 다시 입력하면 같은 레코드를 실수로 추가하지 않도록 거부한다. 처음부터 반복하려면 `quit` 후 프로그램을 다시 실행하면 된다.

아직 study 그룹은 커밋하지 않았다. `status`는 이때 `committed=none`, `lag=unknown`으로 표시한다. 커밋을 한 번도 저장하지 않은 상태와 offset 0을 저장한 상태를 구별하기 위해서다.

## 5. 실험 2 — 읽었지만 커밋하지 않은 위치는 어떻게 되는가

두 레코드를 읽되 커밋하지 않는다.

```text
read study 2 no-commit
```

실제 반환 결과는 다음과 같았다.

```text
read group=study value=A offset=0
read group=study value=B offset=1
result group=study read=[0, 1] position=2 committed=null
```

A와 B는 읽었다. 현재 Consumer의 다음 위치는 2다. 그러나 그룹에는 저장한 위치가 없다. 명령이 끝나면 이 Consumer 객체는 닫힌다. 이제 새 Consumer로 다시 읽으며 커밋한다.

```text
read study 2 commit
```

```text
read group=study value=A offset=0
read group=study value=B offset=1
result group=study read=[0, 1] position=2 committed=2
```

두 번째 Consumer도 A와 B를 읽었다. 이전 객체의 position=2를 이어받은 것이 아니다. 저장된 커밋이 없고 시작 위치 정책이 earliest이므로 이 실습의 첫 유효 위치인 0에서 시작했다.

이 결과는 “읽기”와 “이어 읽을 위치 저장”이 별개의 동작임을 보여 준다. Kafka에 같은 레코드를 두 번 저장한 실험도 아니다. Producer가 넣은 것은 여전히 A, B, C 세 개이며, 읽는 쪽에서 0과 1을 다시 가져온 것이다.

여기서 재현한 것은 **Consumer를 닫고 새로 만들었을 때의 동작**이다. 프로세스를 강제 종료하거나 브로커를 재시작한 실험은 하지 않았다. 다만 외부 DB 반영 후 커밋 전에 중단되는 상황을 생각할 때, 이 분리가 왜 업무 중복 문제로 이어질 수 있는지 이해하는 출발점이 된다.

## 6. 실험 3 — earliest인데 왜 처음부터 읽지 않을까

study의 커밋은 이제 2다. 한 레코드를 더 읽고 커밋한다.

```text
read study 1 commit
read study 1 no-commit
status study
```

첫 명령은 C만 읽고, 두 번째 명령은 아무 레코드도 반환하지 않았다.

```text
result group=study read=[2] position=3 committed=3
result group=study read=[] position=3 committed=3
status group=study start=0 end=3 committed=3 lag=0
```

설정은 여전히 `auto.offset.reset=earliest`다. 그런데 A부터 다시 시작하지 않았다. 이 설정은 매번 처음부터 읽으라는 명령이 아니다. 저장된 오프셋이 없거나, 저장된 오프셋을 더 이상 사용할 수 없을 때 시작 위치를 정하는 정책이다. 유효한 커밋이 있으면 그 위치를 사용한다. [Consumer 설정 문서의 auto.offset.reset](https://kafka.apache.org/41/configuration/consumer-configs/)

이번 결과에서 첫 read는 커밋 2로부터 C를 읽었고, 다음 read는 커밋 3으로부터 시작했다. 이 시점에 새 레코드가 없으므로 읽을 것이 없다. 실습 명령은 이미 존재하는 데이터까지만 읽도록 작성되어 있어 즉시 결과를 반환한다. 일반 Consumer가 계속 `poll()`하며 새 데이터를 기다리는 루프와는 실행 방식이 다르다.

## 7. 실험 4 — 다른 그룹은 기존 레코드를 읽을 수 있다

study가 끝까지 읽은 다음, 다른 그룹 이름을 사용한다.

```text
read audit 3 commit
```

```text
result group=audit read=[0, 1, 2] position=3 committed=3
```

audit은 A, B, C를 모두 읽었다. study가 커밋했다고 레코드가 삭제된 것은 아니었다. 이 예제에서는 같은 토픽·파티션의 로그를 두 그룹이 공유하고, 각 그룹은 자신의 이어 읽기 위치를 저장한다.

이 구조를 서비스에 적용하면 같은 글 발행 이벤트를 조회 서비스와 알림 서비스가 각자 읽는 구성을 생각할 수 있다. 각 서비스의 진행 위치를 구분하려면 그룹 ID를 설계해야 한다. 다만 이 예제는 두 그룹을 순서대로 실행했으며, 여러 Consumer가 동시에 파티션을 나눠 처리하는 확장성 실험은 하지 않았다.

## 8. 실험 5 — seek와 commit도 다른 작업이다

이번에는 study의 Consumer를 offset 0으로 옮겨 다시 읽는다.

```text
seek study 0 3
status study
read study 1 no-commit
```

seek 명령은 다음과 같이 구현되어 있다.

```java
consumer.assign(List.of(partition));
consumer.seek(partition, 0L);
// 이후 poll()로 읽되 commitSync()는 호출하지 않는다.
```

실제 결과는 A, B, C의 재소비였다. 하지만 저장된 커밋은 그대로였다.

```text
result group=study read=[0, 1, 2] position=3 committed=3
status group=study start=0 end=3 committed=3 lag=0
result group=study read=[] position=3 committed=3
```

seek는 현재 Consumer의 읽기 위치를 바꾼다. 이 프로그램은 seek 명령에서 커밋하지 않으므로 study의 저장 위치 3은 바뀌지 않는다. 다음 read는 새 Consumer를 만들기 때문에 다시 3부터 시작한다. [KafkaConsumer API의 seek](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

이번 코드는 “현재 객체에서 다시 읽기”와 “그룹이 다음 실행에서 시작할 위치를 바꾸기”를 분리해서 볼 수 있게 작성했다. 운영에서 그룹 오프셋을 되돌리면 외부 DB나 알림 같은 업무를 다시 실행할 수 있다. 실제 업무를 재처리할 때는 레코드가 남아 있는지와 함께 중복 반영을 감당할 수 있는지도 검토해야 한다.

## 9. 코드에서 확인할 부분은 세 곳이다

첫 번째는 Consumer 설정이다.

```java
props.put("enable.auto.commit", "false");
props.put("auto.offset.reset", "earliest");
props.put("max.poll.records", "1");
```

자동 커밋을 꺼서 명시적으로 호출한 커밋만 관찰한다. `max.poll.records=1`은 한 번의 poll이 돌려주는 개수를 제한해 단계별 출력과 처리를 따라가기 쉽게 한다. 브로커가 네트워크에서 반드시 한 레코드씩만 전송한다는 뜻은 아니다. [Consumer 설정 문서의 max.poll.records](https://kafka.apache.org/41/configuration/consumer-configs/)

두 번째는 처리 후 커밋하는 부분이다. 아래는 핵심 로직을 줄여 쓴 코드이며 전체 소스에는 시간 제한과 결과 검증이 포함되어 있다.

```java
for (var record : consumer.poll(Duration.ofMillis(250))) {
    System.out.println(record.value());
    if (commit) {
        consumer.commitSync(Map.of(
            partition, new OffsetAndMetadata(record.offset() + 1)
        ));
    }
}
```

이 예제는 각 출력 직후 해당 레코드의 다음 위치를 커밋한다. 자동 검증에서 A와 B를 읽고 나면 2가 저장되는지 확인한다. 마지막 poll에서 받아 온 모든 데이터를 처리하지 않았는데 단순히 현재 position을 커밋하는 식으로 고치면, 처리하지 않은 데이터를 지나친 위치를 저장할 수 있다. 배치 처리를 추가할 때는 실제 처리 완료 경계를 따져야 한다.

세 번째는 read 메서드의 try-with-resources다. Consumer를 메서드 안에서 생성하고 종료한다. 덕분에 각 read의 결과가 이전 객체의 메모리 상태 때문인지, 그룹에 저장된 오프셋 때문인지 구별할 수 있다. 브로커까지 매번 새로 만들면 커밋도 사라져 다른 실험이 되므로, 브로커의 수명은 바깥쪽 실습 세션에 둔다.

## 10. 커밋과 보존 정책을 혼동하지 말자

이번 관측에서는 두 그룹이 모두 커밋한 뒤에도 로그 시작 0과 끝 3이 유지됐고, seek로 세 레코드를 다시 읽었다. 이 결과로 확인한 것은 소비와 커밋이 이 레코드들을 제거하지 않았다는 점이다.

그렇다고 레코드가 언제나 영구 보관된다는 뜻은 아니다. Kafka의 `cleanup.policy=delete`에서는 시간·크기 보존 정책에 따라 오래된 로그 세그먼트를 정리한다. `compact` 정책은 키별 최신 값을 보존하는 방식으로 작동한다. 정리는 단순히 “모든 Consumer가 읽었는가”만으로 결정되지 않는다. 보존 기간과 세그먼트 정리 시점도 구분해야 한다. [Topic 설정 문서](https://kafka.apache.org/41/configuration/topic-configs/)

이 예제의 브로커에는 24시간 보존 설정을 넣었지만 실제로 24시간 기다리거나 세그먼트 삭제를 검증하지 않았다. `quit` 후 데이터가 사라지는 것은 실습 도구가 임시 브로커를 정리하기 때문이다. 이를 Kafka의 일반적인 재시작 동작이나 보존 기간 만료 결과로 해석하면 안 된다.

또한 `lag=0`은 이 실습에서 `end - committed = 3 - 3`이라는 뜻이다. 외부 DB의 내용이 정확하다거나 알림 발송이 모두 성공했다는 증명이 아니다. 이 프로그램은 콘솔 출력만 처리로 간주하기 때문이다. 일반 환경에서는 오프셋에 빈 구간이나 트랜잭션 관련 레코드가 있을 수 있어 오프셋 차이를 항상 정확한 업무 메시지 개수로 취급하는 것도 피해야 한다.

## 11. 자동 검증 결과와 적용 범위

다음 명령은 새로운 로컬 브로커에서 실험을 처음부터 수행한다.

```bash
./gradlew :kafka:01-log-and-offset:verifyLab --console=plain
```

작성 시 실제 실행에서 다음 7개 시나리오가 통과했다. 표는 반환 오프셋과 해당 Consumer의 읽기 종료 시점 상태다.

| 시나리오 | 반환 offset | position | committed |
|---|---|---|---|
| study: 2개 읽고 커밋하지 않음 | 0, 1 | 2 | 없음 |
| study: 새 Consumer로 2개 읽고 커밋 | 0, 1 | 2 | 2 |
| study: 이어서 1개 읽고 커밋 | 2 | 3 | 3 |
| study: 다시 읽기 | 없음 | 3 | 3 |
| audit: 처음 읽기 | 0, 1, 2 | 3 | 3 |
| study: 0으로 seek 후 커밋 없이 읽기 | 0, 1, 2 | 3 | 3 |
| study: seek 이후 새 Consumer로 읽기 | 없음 | 3 | 3 |

마지막에는 로그 경계가 `[0, 3)`인지도 검증한다. 예상과 다르면 AssertionError가 발생하여 Gradle 작업이 실패한다. 자동 검증의 PASS는 위 조건의 일치를 의미한다. 여러 브로커의 복제 내구성, 리밸런스, 네트워크 단절, 실제 DB 중복 방지나 처리량을 검증했다는 뜻은 아니다.

공개 코드의 [검증 기록](https://github.com/BaekChan1024/my-minimal-playground/blob/kafka-01-v1/kafka/01-log-and-offset/VERIFICATION.md)에는 실행 범위와 결과를 별도로 남겼다. 특정 숫자 자체를 외우기보다, 왜 position과 committed가 달라졌다가 같아지는지를 따라가 보자.

## 12. 직접 바꿔 보고 설명하기

`quit` 후 실습을 다시 시작한다. 이번에는 두 레코드 대신 한 레코드만 읽는다.

```text
seed
read study 1 no-commit
read study 1 commit
read study 1 no-commit
```

각 명령을 실행하기 전에 반환할 값과 committed를 적어 보자. 두 번째 명령은 첫 번째와 같은 레코드를 읽을까? 세 번째 명령은 어느 위치에서 시작할까? 예상이 틀렸다면 새 Consumer가 시작할 때 무엇을 참조했는지 다시 확인하면 된다.

마지막으로 다음 상황을 생각해 보자. Consumer가 글 조회 데이터를 DB에 반영한 뒤, Kafka 오프셋을 커밋하기 전에 중단됐다. 다시 읽을 가능성이 있는 레코드는 무엇이며, DB에는 어떤 문제가 생길 수 있을까? 이번 실습은 외부 DB를 사용하지 않았으므로 그 결과를 검증했다고 주장할 수는 없다. 그러나 읽기 위치와 저장 위치를 구분했다면, 후속 Outbox·Inbox 실험에서 어느 실패 구간을 확인해야 하는지 설명할 수 있다.

이 글과 예제는 AI의 코드 작성·실행 검증·원고 작성 지원을 받아 제작했다. 본문은 확인한 실행 결과와 공식 문서를 근거로 하며, 학습자가 아직 수행하지 않은 실습이나 회고를 경험담으로 포함하지 않았다.
