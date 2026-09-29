# 예상·실행·비교

1. README의 일곱 조건에서 최종 글·Outbox·시도 기록 행 수와 호출자가 받을 예외를 예상하세요.
2. `./play-spring-01`을 실행하고 실제 출력과 비교하세요. 성공 여부보다 예상이 달랐던 조건을 먼저 설명하세요.
3. 같은 Fault인데 external-runtime과 self-invocation의 결과가 다른 이유는 무엇인가요?
4. caught-local과 caught-required 모두 catch가 있는데 최종 결과가 왜 다른가요?
5. differentTx와 outerResumed는 무엇을 비교한 값인가요?
6. audit 행이 남았다는 사실을 글 작성 성공으로 해석해도 되나요?
7. 이 실험에서 SQL 오류, JPA flush 실패, Kafka 전송, 연결 풀 고갈까지 실행했나요?
