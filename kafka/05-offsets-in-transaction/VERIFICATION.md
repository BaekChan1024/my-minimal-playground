# Kafka 05 검증 기록

검증일: 2026-09-27. macOS ARM64, JDK 21, Gradle 9.2.1. 잠금 파일의 Kafka broker/client 4.1.1, spring-kafka-test 4.0.1. 실행 출력도 클라이언트 4.1.1을 확인한다.

## 실행한 검증

- `./play-kafka-05 verify`: 아래 다섯 조건 통과, 종료 코드 0.
- 저장소 밖에서 `Kafka-05.command`: Enter 두 번의 전체 안내 비교 통과, 종료 코드 0.
- 안내 실습에서 Enter 후 q 입력: 별도 커밋 비교를 마친 뒤 트랜잭션 단계 전에 정상 종료, 종료 코드 0.
- `bash -n play-kafka-05 Kafka-05.command`, `git diff --check` 통과.

초기 구현은 Admin의 default.api.timeout.ms가 기본 request.timeout.ms보다 짧아 초기화에 실패했다. request.timeout.ms=5000, default.api.timeout.ms=10000으로 수정한 최종 코드에서 위 검증을 통과했다. 이를 운영 장애나 트랜잭션 실패 실험으로 해석하지 않는다.

```text
PASS separate-gap: position=1 inputCommitted=null outputRC=1 attempts=1
PASS separate-retry: inputCommitted=1 outputRC=2 attempts=2
PASS atomic-abort: inputCommitted=null outputRC=0 outputRU=1 attempts=1
PASS atomic-commit: inputCommitted=1 outputRC=1 outputRU=2 attempts=2
PASS atomic-restart: position=1 inputEnd=1 replayed=0
DONE: separate commit gap versus Kafka offset/output transaction verified; business code ran twice in both cases.
```

입력은 orders P0 offset 0 한 건이다. 각 그룹에서 실제 subscribe와 유효한 groupMetadata를 사용한다.

입력은 실제 poll 결과의 topic·partition·offset·key·value·다음 오프셋을 검사한다. 출력은 Producer 응답의 오프셋과 Consumer가 읽은 key/value/순서를 대조한다. Admin API로 입력 그룹 커밋의 없음/1을 확인한다. abort 직후 같은 Consumer position이 1인 것도 검사한다. 최종 재시작은 실제 할당·position·endOffsets와 빈 poll을 함께 검사한다.

출력 조회는 수동 할당으로 처음부터 읽되 RC/RU 경계가 기대 위치 이상이 될 때까지 제한 시간 안에서 관측한다. 고정 sleep이 끝났다는 이유로 성공 처리하지 않는다. 질문은 Consumer와 트랜잭션이 닫힌 단계 사이에만 표시한다.

## 한계

강제 종료가 아닌 정상 close/reopen과 명시적인 abort다. Producer는 트랜잭션 두 시도 동안 유지한다. 다중 입력/출력 파티션, 다중 브로커, fencing, 리밸런스 중 실패, Producer 재시작 복구, commit 응답 유실, 외부 DB/HTTP, 성능을 검증하지 않았다. attempts는 Java 정수로 센 처리 시도 수이며 외부 업무 성공 수가 아니다. Admin 조회와 출력 조회는 분산된 상태를 하나의 원자적 스냅샷으로 읽는 실험이 아니다.

RF/min ISR=1은 단일 브로커 실습용이다. Windows/Linux 및 선택 연습 변형은 실행하지 않았다. 메타데이터 포맷 로그의 metadata.version을 의존성 버전으로 해석하지 않는다. 기존 01~04 실습은 이번에 재실행하지 않았다.
