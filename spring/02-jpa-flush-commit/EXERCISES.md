# 실행 전 예상

아래 답을 먼저 적고 `./play-spring-02` 결과와 비교하세요.

1. IDENTITY ID가 할당된 직후, 같은 연결과 다른 연결에서 각각 몇 행을 볼까요? 롤백 후에는요?
2. 바깥 서비스가 @Transactional인 경우 saveAndFlush 반환만으로 커밋됐다고 할 수 있을까요?
3. 조회한 관리 엔티티의 제목만 바꾸고 save를 호출하지 않으면 flush에서 UPDATE가 실행될까요? 버전 0은 언제 1이 될까요?
4. 제목을 바꾼 뒤 같은 엔티티를 대상으로 JPQL count를 실행하면 이전 제목을 기준으로 셀까요?
5. UNIQUE 위반 실험에서 beforeFlush와 afterFlush 중 어느 표시까지 남을까요?
6. 지연 FK 실험에서 flushSucceeded;bodyReturning이 남았는데 호출자에게 예외가 발생할 수 있을까요? 최종 제목과 outbox 행 수를 예상하세요.

추가 생각: flush 직후 Kafka를 직접 전송하면 6번 실패에서 어떤 불일치가 가능할까요? 이 실습은 Kafka 전송을 실행하지 않습니다.
