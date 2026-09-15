# Kafka 02 검증 기록

2026-09-15, macOS ARM64, JDK 21. Kafka 의존성 4.1.1, spring-kafka-test 4.0.1,
Gradle 9.2.1. 의존성은 이 모듈의 gradle.lockfile에 고정되어 있습니다.

## 실행과 확인

- `./play-kafka-02 verify`: 7단계 모두 통과, 종료 코드 0.
- 저장소 밖에서 `Kafka-02.command`에 Enter 7개를 입력: 안내 7단계와 최종 검증 완료, 종료 코드 0.
- 첫 단계를 완료하고 두 번째 안내에서 q 입력: 다음 단계 생산 없이 기존 Consumer가 회수되고 종료됨.
- `bash -n play-kafka-02 Kafka-02.command`: 통과.
- 기존 `./play verify`: Kafka 01의 7개 시나리오 통과.
- Finder 더블클릭 화면 자체는 검증하지 않았습니다. 실행 스크립트를 저장소 밖에서 호출하는 경로를 확인했습니다.

## 검증하는 내용

- 실제 Producer의 전송 결과 partition이 예상 키 매핑과 일치.
- 그룹 안정 상태에서 파티션 0/1/2가 중복·누락 없이 한 번씩 할당됨.
- 1/2/3/4 Consumer의 할당 개수가 각각 [3], [1,2], [1,1,1], [0,1,1,1].
- 각 단계의 레코드를 그 파티션의 담당 Consumer가 받았음.
- audit 그룹이 기존 36개를 독립적으로 읽고 두 그룹 모두 최종 54개를 받음.
- 정상 종료한 멤버를 제외한 세 멤버가 모든 파티션을 맡고 커밋 이후부터 이어 읽음.
- 같은 키 9개는 안정 상태의 한 멤버에게만 전달됨.
- 보낸 레코드의 키·값·파티션·offset과 각 그룹의 받은 목록이 같음.
- 같은 키의 수신 순서 및 각 파티션의 최종 committed가 다음 위치와 일치.

합계만 맞추는 검증이 아닙니다. 고유 레코드 목록·순서·커밋을 각각 비교하며,
실패 또는 시간 초과 시 예외를 발생시킵니다. 처리 속도는 비교하지 않았습니다.

## 범위와 한계

실험의 업무 처리는 메모리 관측 목록 구성입니다. DB나 외부 API는 없습니다.
멤버 변경 전에 기존 데이터 처리를 완료하고 커밋하며, 할당 안정 후 다음 입력을 보냅니다.
exactly-once, 처리 중 재할당, 강제 종료, 다중 브로커 복제, 여러 머신의 네트워크,
consumer 프로토콜, 협력적 할당, 파티션 증설, 키 없는 입력은 검증하지 않았습니다.
자동·안내 실험은 AI가 실행했습니다. 사용자 학습 완료를 의미하지 않습니다.
EXERCISES의 마지막 boolean 조건 변경은 학습용 반례 제안이며, 위 정상 실행 기록에 포함하지 않습니다.

## 실제 자동 실행 출력 발췌

초기화 경로·컴파일 안내·종료 정리 출력은 제외했습니다. 서로 다른 스레드의 콜백 출력 순서는 달라질 수 있습니다.

```text
Kafka 4.1.1 | partitions=3 | classic + RangeAssignor | temporary broker
Synthetic key map (default Java producer, UTF-8, fixed partition count): {0=order-2, 1=order-0, 2=order-4}

1/7 Consumer 1개
assigned study-1 [0, 1, 2]
assignment group=study {study-1=[0, 1, 2]}
sent phase=1 records=9 mode=three-keys
processed group=study phase=1 {study-1=9}
PASS: members=1 partition-counts=[3]

2/7 Consumer 2개
revoked study-1 [0, 1, 2]
assigned study-1 [0, 1]
assigned study-2 [2]
assignment group=study {study-1=[0, 1], study-2=[2]}
sent phase=2 records=9 mode=three-keys
processed group=study phase=2 {study-1=6, study-2=3}
PASS: members=2 partition-counts=[1, 2]

3/7 Consumer 3개
revoked study-2 [2]
revoked study-1 [0, 1]
assigned study-3 [2]
assigned study-1 [0]
assigned study-2 [1]
assignment group=study {study-1=[0], study-2=[1], study-3=[2]}
sent phase=3 records=9 mode=three-keys
processed group=study phase=3 {study-1=3, study-2=3, study-3=3}
PASS: members=3 partition-counts=[1, 1, 1]

4/7 Consumer 4개
revoked study-1 [0]
revoked study-3 [2]
revoked study-2 [1]
assigned study-4 []
assigned study-3 [2]
assigned study-2 [1]
assigned study-1 [0]
assignment group=study {study-1=[0], study-2=[1], study-3=[2], study-4=[]}
sent phase=4 records=9 mode=three-keys
processed group=study phase=4 {study-1=3, study-2=3, study-3=3, study-4=0}
PASS: members=4 partition-counts=[0, 1, 1, 1]

5/7 다른 그룹 audit
assigned audit-1 [0, 1, 2]
assignment group=audit {audit-1=[0, 1, 2]}
PASS: independent group audit replayed 36 records

6/7 담당 Consumer 정상 종료
assignment group=study {study-1=[0], study-2=[1], study-3=[2], study-4=[]}
Leaving gracefully: study-1 partitions=[0]
revoked study-1 [0]
revoked study-3 [2]
revoked study-4 []
revoked study-2 [1]
assigned study-4 [2]
assigned study-2 [0]
assigned study-3 [1]
assignment group=study {study-2=[0], study-3=[1], study-4=[2]}
sent phase=5 records=9 mode=three-keys
processed group=study phase=5 {study-2=3, study-3=3, study-4=3}
PASS: graceful leave: remaining 3 members cover all partitions and resume

7/7 같은 키에 데이터 집중
sent phase=6 records=9 mode=one-key
processed group=study phase=6 {study-2=9, study-3=0, study-4=0}
PASS: hot key: 9 records handled by one member
PASS: both groups received all 54 unique records; per-key order and committed offsets verified
DONE: 7 steps verified. This is an assignment experiment, not a speed benchmark or crash test.
```
