# Kafka 04 검증 기록

검증일: 2026-09-27. macOS ARM64, JDK 21, Gradle Wrapper 9.2.1. Kafka broker/client 의존성은 잠금 파일의 4.1.1, spring-kafka-test는 4.0.1이다. 실제 클라이언트 출력도 4.1.1이다.

## 실행한 검증

- `./play-kafka-04 verify`: 실제 임시 브로커에서 open·abort·commit 비교 통과, 종료 코드 0.
- 저장소 밖에서 `Kafka-04.command`: Enter 두 번의 안내 실행 전체 통과, 종료 코드 0.
- 첫 질문 Enter, 두 번째 질문 q: open·abort까지만 실행하고 정상 종료, 종료 코드 0.
- `bash -n play-kafka-04 Kafka-04.command`, `git diff --check` 통과.

실제 핵심 출력:

```text
PASS open: RU offsets=[0, 1] end=2; RC offsets=[] end=0; externalEffects=1
PASS abort: RU offsets=[0, 1]; RC offsets=[1]; externalEffects=1
PASS commit: RU offsets=[0, 1, 3, 4]; RC offsets=[1, 3, 4]; sameEventCopies=2; externalEffects=2
DONE: open/abort/commit visibility and two explicit sends verified. Consumer group offset transactions are not tested.
```

RU=`read_uncommitted`, RC=`read_committed`다. 매번 수동 할당한 P0의 처음부터 읽는다. Producer 응답과 Consumer 레코드의 topic·partition·offset·key·value를 대조하며 각 읽기 목록의 순서를 검사한다. 개수만 맞는 것으로 성공 처리하지 않는다. 읽기 경계는 endOffsets로 확인하며 읽기·전송·트랜잭션 API에 시간 상한을 둔다.

실습은 트랜잭션 전송과 일반 전송을 같은 파티션에 배치한다. 진행 중인 트랜잭션이 있는 동안 RU와 RC의 endOffsets 차이를 검사하고 RC의 poll 반환이 비어 있으며 position=0인지 확인한다. abort 후 RC 경계가 일반 메시지를 포함할 때까지 관측한 다음 데이터 정체성을 검증한다. 같은 key/value의 명시적인 두 send가 서로 다른 오프셋에 남는지도 확인한다.

`externalEffects`는 Java 정수이며 외부 DB나 HTTP 호출이 아니다. 네트워크 실패·Producer 자동 재시도·트랜잭션 타임아웃·fencing·다중 브로커·다중 파티션·그룹 오프셋 트랜잭션·end-to-end exactly-once·성능을 검증하지 않았다. RF/min ISR=1은 로컬 재현용이다. offset 2의 내부 타입을 로그 덤프로 검사하지 않았다. Linux/Windows 및 선택 연습의 세 번째 send 변형은 미검증이다.

메타데이터 포맷 로그의 `metadata.version`은 의존성 버전과 같은 이름의 값이 아니다. 버전 판정은 잠금 파일과 클라이언트 버전 출력을 사용했다. 테스트 출력을 사용자 숙지 완료로 해석하지 않는다.
