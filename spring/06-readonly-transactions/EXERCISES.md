# 실행 전 예상

1. readOnly 트랜잭션에서 `find()`로 읽은 엔티티의 제목을 바꾸고 메서드가 끝나면 예외가 날까요, 저장될까요, 아무 일도 없을까요?
2. 같은 상황에서 `em.flush()`를 직접 호출하면 UPDATE가 실행될까요?
3. readOnly 트랜잭션에서 `JdbcTemplate.update()`로 UPDATE를 보내면? `em.persist()` 후 flush하지 않으면? flush하면?
4. 쓰기 트랜잭션 메서드가 `@Transactional(readOnly = true)` 메서드를 호출하면(REQUIRED) 안쪽은 읽기 전용이 될까요? 반대 방향은요?
5. `AbstractRoutingDataSource`가 `isCurrentTransactionReadOnly()`로 replica를 고르면 readOnly 트랜잭션은 replica로 갈까요?
6. readOnly 트랜잭션에서 PostgreSQL이 쓰기를 거부하는 것은 Spring이 막기 때문일까요? 무엇을 바꾸면 거부하지 않을까요?
7. open-in-view처럼 한 요청에서 EntityManager를 공유할 때 readOnly 트랜잭션 안에서 바꾼 엔티티는 어떻게 될까요?
8. 엔티티 2만 개를 읽을 때 readOnly는 무엇을 줄일까요? 로딩 중 할당량, 유지되는 메모리, 커밋 시간을 각각 예상하세요.

조건 변경: `Jpa`의 `HibernateJpaVendorAdapter`에 `setPrepareConnection(false)`를 적용하면 flags 결과가 어떻게 바뀔지 예상한 뒤 실습 복사본에서 확인해 보세요. 기존 assert는 원래 구성을 기대하므로 실패할 수 있습니다.
