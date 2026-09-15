# Kafka Consumer를 늘리면 더 빨라질까? 파티션·키·Consumer Group 이해하기

주문 이벤트를 읽는 Consumer 한 개가 점점 밀리기 시작했다. 가장 먼저 떠오르는 해결책은 Consumer를 더 띄우는 것이다. 그런데 두 개로 늘렸을 때는 일이 나뉘더니, 네 개로 늘리자 하나는 아무 메시지도 받지 않는다. 어떤 날에는 세 개가 모두 살아 있는데도 한 개만 계속 바쁘다.

이것은 장애일 수도 있지만, Kafka의 분배 방식에 따른 정상적인 결과일 수도 있다. Consumer 개수만으로 병렬 처리 수준을 판단할 수 없는 이유는 **파티션이 작업을 나누는 단위이고, 키가 레코드를 어느 파티션에 넣을지 결정하기 때문**이다.

위 상황은 문제를 설명하기 위한 가정이다. 이 글에서는 실제 로컬 Kafka에 파티션 3개를 만들고 Consumer 수를 1→2→3→4개로 바꾸어 확인한다. 다른 그룹의 재소비, 담당 Consumer의 정상 종료, 한 키에 몰린 입력까지 관찰한다. 처리량이나 지연 시간을 측정한 성능 실험은 아니다.

앞선 [로그·오프셋·커밋 실습](https://blog.baekchan.com/post/kafka-첫-실습-읽은-메시지는-사라질까-로그오프셋커밋-이해하기)과 [position·seek·commit·ack 보충 글](https://blog.baekchan.com/post/kafka의-positionseekcommitack-읽은-위치와-처리-완료는-어떻게-다를까)에서는 읽기 위치와 저장된 재시작 위치를 구분했다. 이번에는 그 위치가 여러 파티션과 Consumer 사이에서 어떻게 나뉘는지 살펴본다.

## 1. 토픽은 이름이고, 파티션은 실제로 나누어 읽는 로그다

Topic은 주문 이벤트처럼 관련 레코드를 모으는 논리적인 이름이다. Partition은 토픽 안에서 레코드를 순서대로 저장하는 로그다. 파티션이 세 개라면 다음처럼 각각의 로그가 생긴다.

```text
Topic: playground-groups

P0: offset 0 → offset 1 → offset 2 → ...
P1: offset 0 → offset 1 → offset 2 → ...
P2: offset 0 → offset 1 → offset 2 → ...
```

P0의 offset 1과 P1의 offset 1은 서로 다른 레코드다. offset을 이야기할 때 토픽과 파티션을 함께 알아야 하는 이유다. Kafka에서 레코드의 순서는 파티션 안에서 해석한다. P0의 offset 2가 P1의 offset 1보다 업무상 나중에 발생했다고 번호만으로 판단할 수 없다. [Kafka의 로그·소비 위치 설계](https://kafka.apache.org/41/design/design/)

파티션을 나누면 여러 Consumer에게 서로 다른 로그를 맡길 수 있다. 그 대신 토픽 전체의 단일 순서는 자동으로 제공되지 않는다. “병렬 처리하고 싶다”는 요구와 “어느 범위의 순서를 지켜야 하나”라는 요구를 함께 검토해야 한다.

예를 들어 같은 주문의 생성→결제→취소는 순서가 중요할 수 있다. 반면 서로 다른 주문까지 반드시 한 줄로 처리해야 하는지는 별도의 업무 판단이다. 이 차이가 키를 선택하는 기준이 된다.

## 2. 키는 Consumer를 고르는 값이 아니다

Producer는 레코드를 보낼 때 키를 넣을 수 있다. 기본 Java Producer에서 파티션을 명시하지 않고 키를 제공하면, 직렬화된 키를 바탕으로 파티션을 선택한다. 키를 직접 특정 Consumer 이름에 연결하는 것은 아니다. [Kafka Producer 파티션 선택 설정](https://kafka.apache.org/41/configuration/producer-configs/#partitioner.class)

```text
키 → Producer가 선택한 파티션 → 그 파티션을 현재 담당하는 Consumer
```

따라서 Consumer가 바뀌어도 기존 레코드의 파티션과 offset은 그대로다. 같은 파티션의 다음 담당자가 그 로그를 이어 읽는다.

실습에서는 세 파티션의 분배가 눈에 보이도록 각 파티션으로 향하는 합성 키를 하나씩 골랐다. 실제 Producer 전송 결과에서 확인한 매핑은 다음과 같다.

| 문자열 키 | 파티션 |
|---|---|
| order-2 | P0 |
| order-0 | P1 |
| order-4 | P2 |

이름의 끝 숫자를 파티션 번호로 쓰지 않는다. `order-2`가 P0에 들어간 사실만 봐도 문자열 키의 해시를 사용하는 것과 단순 숫자 나머지를 사용하는 것이 다름을 알 수 있다.

[공개 실습 코드](https://github.com/BaekChan1024/my-minimal-playground/blob/kafka-02-v1/kafka/02-partitions-and-groups/src/main/java/playground/kafka/GroupLab.java)는 UTF-8 문자열의 Kafka 해시를 이용해 후보 키를 찾는다. 실제 전송에는 파티션 번호를 지정하지 않고, 응답의 partition을 예상값과 비교한다.

```java
var metadata = producer.send(
    new ProducerRecord<>(TOPIC, key, value)
).get(15, TimeUnit.SECONDS);

require(metadata.partition() == expectedPartition,
        "Producer key routing mismatch");
```

여기서 세 키가 고르게 나뉘는 것은 실험을 위해 의도적으로 만든 조건이다. 임의의 키 세 개가 항상 세 파티션으로 흩어지는 것은 아니며, 서로 다른 키가 같은 파티션으로 갈 수도 있다.

같은 키가 같은 파티션으로 간다는 설명도 조건을 포함한다. 동일한 직렬화 결과, 파티션 수, 파티션 선택 정책을 유지하는 경우다. 키를 무시하는 설정이나 사용자 정의 파티셔너를 사용하면 달라질 수 있다. 파티션 수를 늘리면 키의 목적지가 달라질 수 있으므로, “같은 키는 영원히 같은 파티션”이라고 이해하면 안 된다. 과거 레코드를 새 목적지로 자동 재배치하는 것도 아니다.

키가 없는 입력은 이번 실험에 포함하지 않았다. 기본 분배가 항상 메시지 한 개씩 P0→P1→P2를 반복한다고 가정하는 것도 피해야 한다. 키가 없는 입력의 배치·분배 정책은 별도로 살펴볼 주제다.

## 3. 같은 그룹은 파티션을 나누고, 다른 그룹은 각자 읽는다

Consumer Group은 하나의 논리적인 구독을 여러 Consumer가 나누어 수행하는 단위다. 같은 `group.id`로 참여한 Consumer들은 구독한 파티션을 나누어 맡는다. 이 글에서 사용하는 일반 KafkaConsumer 그룹의 안정 상태에서는 한 파티션을 그룹의 한 멤버가 담당한다. [Kafka Consumer Group 설명](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

파티션 3개를 Consumer 2개가 읽는 한 가지 배치는 다음과 같다.

```text
study 그룹
  study-1 : P0, P1
  study-2 : P2
```

이때 P0의 레코드를 study-1과 study-2에게 번갈아 한 개씩 나누어 주는 구조가 아니다. P0의 담당자를 정해 그 로그를 읽게 한다. 한 Consumer가 여러 파티션을 맡는 것은 가능하다.

반면 다른 `group.id`의 Consumer를 띄우면 독립적인 구독이 된다.

```text
study 그룹: P0/P1/P2를 멤버끼리 나누어 읽음
audit 그룹: 같은 P0/P1/P2를 자기 위치에서 읽음
```

로그는 공유하지만 커밋 위치는 그룹별이다. 주문 처리 서비스와 감사 기록 서비스가 각각 모든 이벤트를 봐야 한다면 서로 다른 그룹이 될 수 있다. 주문 처리 서버를 여러 개 띄워 일을 나누려는 목적이라면 같은 그룹으로 묶는 방향이 된다.

파티션의 단일 담당과 장애 시 레코드의 재전달은 다른 문제다. 같은 그룹 안에서 파티션 담당을 겹치지 않게 나눈다고 해서 업무가 반드시 한 번만 실행되는 것은 아니다. 처리 후 커밋 전 장애가 나면 재처리가 생길 수 있다는 앞선 글의 설명은 그대로 적용된다.

## 4. 첫 실습의 assign 대신 subscribe를 사용한 이유

첫 실습은 `assign()`으로 파티션을 직접 지정했다. 읽기 위치와 커밋에 집중하기에는 단순했지만, Kafka가 Consumer 사이의 담당을 자동으로 바꾸는 실험은 아니었다.

이번 실습은 `subscribe()`를 사용해 그룹 관리에 참여한다. Consumer를 추가하거나 정상 종료했을 때 담당 변경을 실제로 관찰하기 위해서다.

```java
props.put("group.id", group);
props.put("client.id", name);
props.put("group.protocol", "classic");
props.put("partition.assignment.strategy", RangeAssignor.class.getName());
props.put("enable.auto.commit", "false");
props.put("auto.offset.reset", "earliest");
```

`group.id`는 어떤 구독의 일을 나눌지 정한다. `client.id`는 실습 출력에서 클라이언트를 구별하는 이름이다. client.id를 지정했다고 영구적인 멤버 신분이나 고정 파티션 담당이 보장되지는 않는다.

실험에서는 `classic` 그룹 프로토콜과 `RangeAssignor`를 명시했다. Kafka 4.1에는 새로운 `consumer` 그룹 프로토콜도 있으므로, 이 실습의 콜백 순서와 재할당 방식을 모든 구성의 공통 동작으로 일반화하지 않기 위해서다. [Kafka 그룹 프로토콜 설정](https://kafka.apache.org/41/configuration/consumer-configs/#group.protocol)

RangeAssignor는 토픽별로 정렬된 파티션을 Consumer들에게 범위로 나누어 준다. 이 실습처럼 토픽이 하나일 때 Consumer 두 개라면 2개와 1개로 나뉜다. 여러 토픽이나 다른 할당 전략에서는 구체적인 분배가 달라질 수 있다. [RangeAssignor 설명](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/RangeAssignor.html)

각 Consumer는 전용 스레드에서 계속 poll한다. 안내 문구에서 Enter를 기다리는 동안에도 Consumer 스레드는 살아 있다. Java Consumer 객체를 여러 스레드가 임의로 공유하지 않고, 종료 요청에 한해 지원되는 `wakeup()`을 사용한다.

## 5. 로컬에서 실행하기

JDK 21과 Git을 준비한다. Gradle Wrapper가 포함되어 있고, 테스트 라이브러리가 실제 임시 Kafka 브로커의 시작과 정리를 맡는다. Docker나 별도 DB는 필요 없다.

처음 저장소를 받는 경우:

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-02-v1
./play-kafka-02
```

이미 첫 실습을 받았다면, 수정 중인 파일을 보존한 상태에서 새 태그를 가져와 선택한다.

```bash
git fetch origin --tags
git switch --detach kafka-02-v1
./play-kafka-02
```

Mac에서는 저장소의 `Kafka-02.command`를 열어도 같은 안내 모드가 실행된다. 매 단계에서 다음 결과를 먼저 예상하고 Enter를 누른다. `q`는 실습 종료다. 처음 실행에는 의존성 다운로드와 브로커 준비 시간이 필요하다.

자동 검증만 실행할 때는 다음 명령을 사용한다.

```bash
./play-kafka-02 verify
```

| 항목 | 이번 실험 조건 |
|---|---|
| Java / Gradle | JDK 21 / Gradle 9.2.1 |
| Kafka 의존성 | broker/client 4.1.1 |
| 임시 브로커 관리 | spring-kafka-test 4.0.1 |
| 토픽 / 파티션 / 복제 계수 | 1 / 3 / 1 |
| Consumer 실행 | 같은 JVM 안의 개별 Consumer와 전용 스레드 |
| 그룹과 할당 | classic / RangeAssignor / subscribe |
| Producer | 기본 키 분배, acks=all, idempotence=true |
| 커밋 | 자동 커밋 비활성, poll 처리 후 nextOffsets 동기 커밋 |
| 검증 환경 | macOS ARM64 |

같은 JVM이라는 배치를 택한 이유는 여러 터미널이나 서버를 준비하지 않고 멤버 변화를 관찰하기 위해서다. 실제 Kafka 네트워크 클라이언트를 사용하지만, 여러 머신의 네트워크 장애를 재현하는 구성은 아니다.

## 6. Consumer를 1→2→3→4개로 늘려 보기

각 단계에서는 할당이 안정된 뒤 레코드 9개를 전송한다. 세 키에 각각 3개씩 보내므로 파티션별 입력도 3개씩이다. 다음 단계로 넘어가기 전에 받은 레코드와 커밋 위치가 모두 맞는지 확인한다.

### 하나의 Consumer는 세 파티션을 모두 읽었다

2026-09-15 실제 실행에서는 다음 출력이 나왔다.

```text
assignment group=study {study-1=[0, 1, 2]}
processed group=study phase=1 {study-1=9}
```

Consumer 하나가 파티션 하나만 읽을 수 있는 것은 아니다. 이 코드에서는 한 스레드가 세 파티션의 poll 결과를 처리한다. 파티션이 여러 개라는 사실만으로 애플리케이션의 업무 처리가 자동으로 여러 스레드에 나뉘지도 않는다.

### 두 번째와 세 번째 Consumer가 들어오면 담당이 나뉘었다

```text
assignment group=study {study-1=[0, 1], study-2=[2]}
processed group=study phase=2 {study-1=6, study-2=3}

assignment group=study {study-1=[0], study-2=[1], study-3=[2]}
processed group=study phase=3 {study-1=3, study-2=3, study-3=3}
```

두 개일 때 작업이 6개와 3개로 나뉜 것은 메시지 개수를 정확히 반으로 나눈 결과가 아니다. 파티션이 2개와 1개로 배정되고, 각 파티션에 레코드가 3개씩 들어 있어서 생긴 결과다.

이 차이가 중요하다. 파티션 하나에 레코드가 대부분 몰려 있다면 파티션 수를 비슷하게 나누어도 실제 업무량은 비슷해지지 않는다.

### 네 번째 Consumer는 파티션을 받지 못했다

```text
assignment group=study {study-1=[0], study-2=[1], study-3=[2], study-4=[]}
processed group=study phase=4 {study-1=3, study-2=3, study-3=3, study-4=0}
```

study-4는 실행 중이지만 담당 파티션이 없다. 파티션 3개를 동일 그룹의 Consumer 4개가 읽는 이 조건에서는 최대 세 멤버에게만 파티션을 줄 수 있다.

어떤 이름의 멤버가 어느 파티션을 맡는지는 출력에서 확인한 배치다. 모든 환경에서 study-4가 반드시 쉬는 것으로 가정하지 않는다. 검증 코드는 특정 멤버 이름보다 파티션의 중복 없는 전체 할당과 분배 수를 확인한다.

| 그룹 멤버 수 | 파티션 수 분배 | 해당 단계의 레코드 처리 건수 |
|---|---|---|
| 1 | 3 | 9 |
| 2 | 2 + 1 | 6 + 3 |
| 3 | 1 + 1 + 1 | 3 + 3 + 3 |
| 4 | 1 + 1 + 1 + 0 | 3 + 3 + 3 + 0 |

이 표는 분배 결과다. 두 배나 세 배 빨라졌다는 측정 결과가 아니다. 실제 처리 속도에는 레코드별 업무 비용, DB와 외부 API, CPU, 브로커 자원 등의 병목이 함께 작용한다.

## 7. 다른 그룹을 추가하면 기존 레코드도 다시 읽는다

앞의 네 단계에서 study 그룹은 총 36개를 읽고 커밋했다. 이때 처음으로 audit 그룹을 추가했다.

```text
assignment group=audit {audit-1=[0, 1, 2]}
PASS: independent group audit replayed 36 records
```

audit에는 저장된 커밋이 없고 실습 설정이 earliest이므로 로그 처음부터 읽었다. study의 커밋을 복사해서 시작하지 않는다.

반대로 같은 study 그룹에 멤버를 추가하면 기존 멤버들과 담당을 나누는 것이 목적이다. “모든 이벤트를 별도로 한 번씩 읽는 새 기능”을 붙이는 것과 “같은 기능의 처리 인스턴스를 늘리는 것”은 group.id 선택부터 다르다.

이 차이는 position과 committed를 배웠던 첫 실습의 연장선이다. 여러 파티션을 읽으면 재시작 위치도 파티션별로 저장되고, 그 위치들의 묶음이 그룹마다 독립적으로 관리된다.

## 8. 담당 Consumer가 떠나면 로그가 아니라 담당이 이동한다

다음 단계에서는 파티션을 맡고 있던 study-1을 정상 종료했다. 종료 전에 기존 입력은 모두 처리하고 커밋한 상태다.

```text
Leaving gracefully: study-1 partitions=[0]
assignment group=study {study-2=[0], study-3=[1], study-4=[2]}
processed group=study phase=5 {study-2=3, study-3=3, study-4=3}
```

남은 세 Consumer가 세 파티션을 맡았다. 앞에서 쉬고 있던 study-4도 이제 파티션을 받았다. P0의 레코드가 P2로 옮겨 간 것이 아니라, 각 로그를 읽을 담당자가 바뀐 것이다.

이러한 담당 변경을 리밸런스라고 부른다. 실습은 `ConsumerRebalanceListener`에서 회수와 할당 콜백을 출력하므로, 새 멤버의 합류나 종료 전후에 어떤 변화가 있었는지 연결해 볼 수 있다. [ConsumerRebalanceListener API](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html)

이번 classic + RangeAssignor 구성에서는 담당을 회수하고 다시 할당하는 과정을 관찰했다. 이 과정에는 기존 멤버도 참여한다. 새 Consumer 하나가 들어왔다고 그 Consumer만 설정하고 끝나는 것은 아니다. 다만 협력적 할당 전략이나 새 consumer 그룹 프로토콜의 동작까지 같은 방식이라고 단정하지 않는다.

프로그램은 Admin이 보고하는 안정 상태와 각 Consumer 콜백의 할당을 대조한다. 그룹의 파티션 0·1·2가 각각 한 번씩 나타나고, 같은 할당을 연속 확인한 뒤 다음 레코드를 전송한다. 단순히 몇 초 자고 결과가 맞기를 기대하는 방식보다 단계의 전제 조건을 분명히 하기 위해서다.

정상 종료와 갑작스러운 장애는 구분해야 한다. 이번에는 실행 중인 Consumer에 종료를 요청하고 닫히기를 기다렸다. kill, 네트워크 단절, 오래 걸리는 업무, 커밋 전 장애를 주입하지 않았으므로 장애 감지 시간이나 미완료 레코드의 재전달 결과를 측정했다고 말할 수 없다.

## 9. Consumer 세 개가 살아 있어도 한 개만 바쁠 수 있다

마지막 단계는 Consumer 수를 바꾸지 않고 입력의 키만 바꾼다. 앞에서는 세 키로 고르게 보냈지만, 이번에는 아홉 레코드 모두 P0으로 향하는 같은 키를 사용한다.

```text
sent phase=6 records=9 mode=one-key
processed group=study phase=6 {study-2=9, study-3=0, study-4=0}
PASS: hot key: 9 records handled by one member
```

세 Consumer가 각각 파티션을 맡고 있어도, 새 레코드가 P0에만 들어가면 P0 담당자가 모두 처리한다. 이런 식으로 특정 키나 파티션에 부하가 몰리는 상황을 hot key 또는 hot partition 문제라고 부른다.

해결책을 검토할 때는 순서 요구부터 돌아보아야 한다. 다음은 이 실험에서 도출한 설계 선택지이며 성능 개선을 측정한 결과는 아니다.

| 선택지 | 기대하는 효과 | 함께 확인할 점 |
|---|---|---|
| 같은 그룹의 Consumer 추가 | 서로 다른 파티션의 담당을 나눔 | 남는 파티션이 있는지, 다른 자원이 병목인지 |
| 파티션 수 증가 | 향후 입력과 담당을 더 나눌 여지 | 키 매핑 변화, 순서 요구, 리밸런스와 운영 비용 |
| 키를 더 세분화 | 큰 키 하나에 몰린 입력을 여러 단위로 나눔 | 원래 키 단위로 필요했던 순서를 잃어도 되는지 |
| 한 Consumer 안에서 업무를 병렬 처리 | 애플리케이션 처리 동시성 조절 | 완료 순서와 안전한 커밋 경계를 별도 관리해야 함 |

예를 들어 모든 주문을 `shop-1`이라는 키로 묶으면 그 가게의 입력이 한 파티션으로 향할 수 있다. 주문별 순서만 필요하다면 주문 ID를 키로 사용하는 방안을 검토할 수 있다. 다만 이것은 업무 요구가 허용할 때 가능한 변경이며, “키를 잘게 나누면 무조건 좋다”는 규칙은 아니다.

파티션을 늘리는 것만으로 단일 hot key가 여러 파티션에 동시에 분산되는 것도 아니다. 기본 키 분배에서 그 키는 바뀐 파티션 수에 따라 하나의 목적지를 선택한다. 더 많은 Consumer를 띄우는 것과 하나의 키를 쪼개는 것은 별개의 문제다.

## 10. 어떤 순서를 확인했고, 무엇을 확인하지 않았나

실습은 한 Producer가 레코드를 순서대로 보내고 전송 완료 응답을 기다린다. Consumer는 poll 결과를 관측값으로 구성한 뒤 해당 결과의 다음 위치를 커밋한다.

```java
c.commitSync(records.nextOffsets());
received.addAll(batch);
```

여기서 batch는 처리한 레코드의 키·값·파티션·offset을 담은 관측 목록이다. 커밋 성공 후 이 목록을 검증 스레드에 전달하므로, 검증이 다음 단계로 넘어갈 때 저장된 위치도 함께 대조할 수 있다. 실제 DB 저장이나 외부 API 호출은 없다.

검증에서는 Producer가 보낸 목록과 각 그룹이 받은 목록을 비교한다. 각 레코드의 식별 정보와 파티션·offset이 일치하는지, 같은 키의 순서가 유지되는지, 최종 커밋이 해당 파티션의 다음 위치인지 확인한다. 단순히 54줄이 출력되었다고 성공 처리하지 않는다.

여섯 번의 생산 단계가 각각 9개이므로 전체 로그에는 54개가 쌓인다. audit은 중간에 합류해 앞의 36개를 읽고, 이후 18개도 독립적으로 읽는다. 실제 실행에서는 두 그룹 각각 54개의 고유 레코드를 관측했고 키별 순서와 커밋 검증을 통과했다.

```text
PASS: both groups received all 54 unique records; per-key order and committed offsets verified
DONE: 7 steps verified. This is an assignment experiment, not a speed benchmark or crash test.
```

이 조건에서 중복 없이 읽었다는 사실은 일반적인 exactly-once 업무 처리를 증명하지 않는다. 멤버 변경 전에 모든 레코드의 처리를 마치고 커밋했으며, 재할당 중에는 새 입력을 보내지 않았다. 실패 구간을 열어 놓는 실험과는 목적이 다르다.

같은 파티션의 로그 순서와 외부 업무의 완료 순서도 다르다. poll 결과를 여러 작업 스레드에 넘기면 뒤 레코드의 업무가 먼저 끝날 수 있다. 이 경우 앞 레코드가 미완료인데 뒤 위치를 커밋하지 않도록 별도의 설계가 필요하다. 이번 코드는 그런 비동기 업무 분배를 사용하지 않는다.

## 11. 직접 확인할 질문과 실험의 한계

실행할 때는 각 단계의 `assignment`와 `processed`를 함께 보자. 전자는 어떤 파티션을 맡았는지, 후자는 그 단계에서 실제로 관측한 레코드가 몇 개인지를 보여 준다. 둘이 같은 정보가 아니라는 점이 이 글의 핵심이다.

1. Consumer 두 개가 6개와 3개를 처리한 것은 무엇을 기준으로 분배했기 때문인가?
2. 같은 그룹에 Consumer 하나를 더 넣는 것과 audit 그룹을 새로 만드는 것은 어떻게 다른가?
3. 파티션 세 개를 세 Consumer가 맡고 있는데도 처리 건수가 9·0·0이 된 이유는 무엇인가?
4. 기존 담당자가 정상 종료한 뒤 이어 읽을 수 있었던 이유를 committed offset과 연결해 설명할 수 있는가?

[실습 문제](https://github.com/BaekChan1024/my-minimal-playground/blob/kafka-02-v1/kafka/02-partitions-and-groups/EXERCISES.md)에는 마지막 생산 단계의 키 분배 조건을 바꾸는 연습을 넣었다. 먼저 예상한 뒤 실행하고, [해설](https://github.com/BaekChan1024/my-minimal-playground/blob/kafka-02-v1/kafka/02-partitions-and-groups/ANSWERS.md)과 비교할 수 있다.

이 글의 코드는 AI가 작성하고 실제 로컬 브로커로 실행·검증했다. 자동 모드와 Mac 실행 도구의 안내 모드를 확인했으며, 사용자 본인의 실행 결과나 이해를 대신 주장하지 않는다. 상세 출력과 범위는 [검증 기록](https://github.com/BaekChan1024/my-minimal-playground/blob/kafka-02-v1/kafka/02-partitions-and-groups/VERIFICATION.md)에 남겼다.

미검증 범위는 다중 브로커의 복제 장애, 여러 머신의 네트워크 장애, 프로세스 강제 종료, 처리 중 리밸런스, DB 부작용, 키 없는 Producer 분배, 파티션 증설, 협력적 할당, 새 consumer 프로토콜, 처리량·지연 벤치마크다. 파티션 3개·단일 토픽·고정 입력이라는 조건 밖의 결과까지 검증한 것으로 읽지 않아야 한다.

Consumer를 늘릴지 결정할 때는 먼저 파티션별 입력과 담당을 살펴볼 수 있다. 분배할 파티션이 남아 있는지, 데이터가 한 키에 몰려 있는지, 업무가 요구하는 순서의 범위가 어디까지인지가 보여야 다음 선택의 이유도 설명할 수 있다.

이전 글: [position·seek·commit·ack — 읽은 위치와 처리 완료 구분하기](https://blog.baekchan.com/post/kafka의-positionseekcommitack-읽은-위치와-처리-완료는-어떻게-다를까)
