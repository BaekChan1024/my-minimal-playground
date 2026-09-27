# 결과 해설

1. RC는 offset 1도 기다립니다. 열린 트랜잭션의 시작 0이 RC의 읽기 끝 경계가 됩니다. RU는 두 전송을 읽습니다.
2. abort 후 RU는 0과 1, RC는 1을 읽습니다. Java 정수는 Kafka 트랜잭션에 포함되지 않아 1로 남습니다. 실제 DB나 HTTP 롤백 실험은 아닙니다.
3. 두 건입니다. 애플리케이션이 명시적으로 두 번 호출한 send는 Producer 내부 재시도와 다릅니다. key/value의 동일성만으로 업무 중복을 제거하지 않습니다.
4. 아닙니다. endOffsets의 결과는 읽기 경계이며 이번 코드는 그룹 오프셋을 저장하지 않습니다. RC의 end=0은 offset 0부터 반환할 수 없다는 배타적 끝 경계입니다.
5. 관측한 간격은 트랜잭션 마커가 로그 오프셋을 차지하지만 두 격리 수준 모두에 반환되지 않는다는 KafkaConsumer 문서와 일치합니다. 직접 로그 세그먼트를 덤프해 offset 2의 타입을 검사하지는 않았습니다.
6. 불가능합니다. Kafka 밖의 원자성은 별도 문제이며, 이 코드에는 입력 소비 위치를 출력과 묶는 sendOffsetsToTransaction도 없습니다.

자료: [KafkaProducer](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html), [KafkaConsumer](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).
