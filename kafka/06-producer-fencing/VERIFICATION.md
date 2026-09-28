# Kafka 06 검증 기록

2026-09-28, macOS ARM64, JDK 21, Gradle 9.2.1. Kafka broker/client 4.1.1과 spring-kafka-test 4.0.1을 의존성 잠금으로 고정했다. 실행 출력의 클라이언트 버전도 확인했다.

- `./play-kafka-06 verify`: 종료 코드 0.
- Enter 후 q로 첫 비교 후 종료: 종료 코드 0, 트랜잭션/브로커 정리.
- 저장소 밖 `/private/tmp`에서 Mac 실행 도구에 Enter 두 번: 전체 안내 실습 종료 코드 0.

두 실행에서 다음 결과를 확인했다.

```text
PASS shared-before: RU=1 oldSendAcknowledged=true
PASS shared-takeover: oldCommit=ProducerFencedException RC=0
PASS shared-final: RC=1 RU=2 sameBusinessKey=true
PASS distinct-final: firstCommit=success secondCommit=success RC=2 RU=2 sameClientId=true
DONE: same-ID takeover fenced the old producer; distinct IDs committed two copies.
```

같은 ID 시나리오에서 old의 send 응답과 RU 레코드를 먼저 확인한다. old가 살아 있고 트랜잭션이 미완료인 채 replacement.initTransactions를 호출한다. 이후 RC가 진행 가능한 끝 경계를 확인하고 old 출력이 보이지 않음을 검사한다. old.commitTransaction의 정확한 예외 타입을 확인한다. 다른 오류나 성공은 테스트 실패다.

최종 RC/RU 레코드는 각 send 응답의 offset·key·value·순서와 대조한다. 고정 sleep의 종료를 성공 근거로 삼지 않으며 endOffsets와 position을 검사한다. 같은 ID의 client.id는 서로 다르고 다른 ID의 client.id는 같다. 두 Producer 객체의 생존 기간은 겹치지만 API 호출 순서는 통제한다.

한계: 실제 프로세스 강제 종료, 네트워크 분할, 다중 브로커·파티션, 세그먼트 덤프, 내부 producer ID/epoch 수치, 자동 timeout 중단, Consumer Group 및 입력 오프셋 트랜잭션, DB/HTTP, 부하, 자동 소유권 재선출을 검증하지 않았다. RF/min ISR=1은 로컬 전용이다. Windows/Linux, 선택 변형 및 기존 01~05 실습은 이번에 실행하지 않았다. metadata.version 로그를 의존성 버전으로 해석하지 않는다.
