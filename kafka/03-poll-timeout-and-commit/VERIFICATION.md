# 검증 기록 — 2026-09-16

실제 로컬 Kafka 임시 브로커에서 실행했다. macOS ARM64, JDK 21, Gradle 9.2.1, broker/client 의존성 4.1.1, spring-kafka-test 4.0.1. 실행의 AppInfoParser 출력도 Kafka 4.1.1이었다. 의존성 잠금 파일을 포함한다.

## 수행한 확인

- `./play-kafka-03 verify`: 두 조건 통과, 종료 코드 0.
- 저장소 밖 디렉터리에서 `Kafka-03.command`에 Enter 두 번: 두 조건 통과, 종료 코드 0.
- 안내 모드에서 첫 질문 Enter, 두 번째 질문 q: 첫 조건만 실행하고 종료 코드 0. try/finally에서 스레드·브로커 정리.
- 새 실행 도구의 `bash -n`과 변경 내용의 공백 오류 검사 통과.

다음은 실제 안내 실행의 실험 결과 줄만 발췌한 것이다. 임시 경로와 무작위 멤버 ID를 제외했다.

```text
effect-only old: offset=0 position=1 committed=null effects=1
effect-only group: only new owns P0; committed=1
effect-only old: late commit rejected (CommitFailedException)
PASS effect-only: newReads=1 effects=2 newPosition=1 committed=1 oldLateCommitFailed=true
committed-first old: offset=0 position=1 committed=1 effects=1
committed-first group: only new owns P0; committed=1
committed-first old: late commit rejected (CommitFailedException)
PASS committed-first: newReads=0 effects=1 newPosition=1 committed=1 oldLateCommitFailed=true
DONE: 2 cases verified; no crash injection, DB transaction, or performance benchmark.
```

탈퇴한 이전 멤버에 대해 coordinator가 멤버를 모른다는 LeaveGroup ERROR 로그가 일부 실행에서 함께 출력됐다. 이 로그의 유무를 성공 조건으로 삼지 않는다. 두 케이스의 관측값과 CommitFailedException, 종료 코드로 판정한다.

## 판정 근거

Producer가 한 번 전송한 레코드의 partition=0/offset=0을 검사한다. A·B가 받은 topic, partition, offset, key, value가 일치하는지 검사한다. Admin이 보고한 그룹 멤버가 B 하나이며 P0을 소유하고, B 로컬 할당·position=1과 그룹 committed=1을 대조한 뒤에 A의 늦은 커밋을 시도한다.

두 조건은 별도 그룹이므로 첫 조건의 커밋이 비교 조건에 섞이지 않는다. 효과 카운터 역시 조건별 새 인스턴스다. 마지막에 B를 정지시키고 Future를 기다린 다음 카운터와 읽기 수를 확인한다. 각 KafkaConsumer는 한 스레드만 사용한다.

## 한계

효과는 메모리 카운터로 표현했다. DB, 결제 API, Inbox, Kafka 트랜잭션, 프로세스 강제 종료, 네트워크 장애, 다중 브로커 복제 장애, 정적 멤버십, 협력적 재할당, 새 consumer 프로토콜은 검증하지 않았다. 2초 설정은 복구시간 측정값이나 운영 추천값이 아니다. Windows·Linux 실행 및 수치 변경 연습도 아직 검증하지 않았다.

자동 모드 결과 이후 버전 출력만 상수 문자열에서 AppInfoParser로 바꾸었고, 최종 코드의 Mac 안내 모드와 q 중도 종료를 실제 실행했다. 처리 로직 변경은 없었다.
