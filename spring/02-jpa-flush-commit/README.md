# Spring 02 — JPA save·flush·commit

상세 글: [Spring 연재](https://blog.baekchan.com/category/spring)

JDK 21과 Git이 필요합니다. 실제 Spring Data JPA 4.0.1, Hibernate 7.2.0.Final, Spring Framework 7.0.2, 임시 PostgreSQL 18.4를 사용합니다. Docker나 기존 DB 설정은 필요 없습니다. 최초 실행 때 Maven Central에서 의존성과 PostgreSQL 바이너리를 받습니다. 임시 서버는 loopback 임의 포트만 사용하고 종료 시 정리합니다. 운영 설정과 자격증명을 읽지 않습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach spring-tx-02-v1
./play-spring-02
```

기존 저장소를 사용한다면 미커밋 변경을 보존한 뒤 태그를 fetch하세요. Mac에서는 `Spring-02.command`를 더블클릭해도 됩니다. Enter로 진행하고 q로 서버 시작 전에 종료합니다. 자동 검증은 `./play-spring-02 verify`입니다. 설치된 JDK 21을 탐색하며 자동 설치하지 않습니다. macOS ARM64에서 검증했고 다른 OS는 미검증입니다.

## 실험 구조

`src/main/java/playground/jpa/JpaLab.java`가 프록시로 감싼 실제 서비스 Bean을 호출합니다. `PostRepository`는 실제 Spring Data JPA 저장소입니다. `Post`는 IDENTITY와 @Version을 사용합니다. 각 시나리오가 끝나면 새 관측 연결에서 DB의 최종 상태를 검사합니다.

- `identity-rollback`: save 직후 같은 트랜잭션/외부 연결 비교, flush 뒤 롤백
- `saveAndFlush-commit`: 외부 서비스 트랜잭션 안에서 saveAndFlush, 서비스 경계 뒤 커밋 확인
- `dirty-flush-rollback`: 관리 엔티티 변경 → JDBC 관측 → flush → 버전 증가 → 롤백
- `auto-query-rollback`: 변경 대상과 겹치는 JPQL이 AUTO flush를 유발하는지 확인
- `unique-at-flush`: 즉시 UNIQUE 제약 위반이 flush에서 발생하는지 확인
- `deferred-at-commit`: 실습용 지연 FK 제약으로 flush 성공 후 커밋 실패 재현

`JdbcTemplate`은 JpaTransactionManager와 같은 DataSource의 트랜잭션 연결을 사용합니다. 외부 관측은 직접 `DataSource.getConnection()`으로 연결하며 autocommit 및 READ_COMMITTED를 검사합니다. JPA 조회가 관측 전에 자동 flush하는 혼동을 피하기 위한 구성입니다. `outbox_record`는 제약 검증용 테이블이고 실제 이벤트 발행이나 Kafka를 실행하지 않습니다.

[먼저 예상하기](EXERCISES.md) → 실행 → [결과 해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
