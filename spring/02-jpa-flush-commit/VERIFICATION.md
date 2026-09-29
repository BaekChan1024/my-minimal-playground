# 검증 기록

검증일: 2026-09-29. macOS ARM64 / JDK 21. Spring Boot 4.0.1 의존성 BOM과 `gradle.lockfile` 사용.

실행: `JAVA_HOME=$(/usr/libexec/java_home -v 21) ./gradlew :spring:02-jpa-flush-commit:verifyLab --write-locks --console=plain`

```text
Spring=7.0.2 Hibernate=7.2.0.Final SpringDataJPA=4.0.1 PostgreSQL=18.4 JDBC=42.7.10
PASS identity-rollback posts=0 row=absent outbox=0 error=RollbackProbe sqlState=none steps=[save:same=1,observer=0,idAssigned=true, flush:observer=0]
PASS saveAndFlush-commit posts=1 row=new:0 outbox=0 error=none sqlState=none steps=[saveAndFlush:observer=0]
PASS dirty-flush-rollback posts=2 row=old:0 outbox=0 error=RollbackProbe sqlState=none steps=[before:same=old,version=0, after:same=new,version=1,observer=old:0]
PASS auto-query-rollback posts=2 row=old:0 outbox=0 error=RollbackProbe sqlState=none steps=[before:same=old,version=0, JPQL:count=1, after:same=new,version=1,observer=old:0]
PASS unique-at-flush posts=2 row=old:0 outbox=0 error=ConstraintViolationException sqlState=23505 steps=[beforeFlush]
PASS deferred-at-commit posts=2 row=old:0 outbox=0 error=DataIntegrityViolationException sqlState=23503 steps=[flushSucceeded;bodyReturning]
ALL 6 SCENARIOS PASSED. 비교 해설: spring/02-jpa-flush-commit/ANSWERS.md
```

실제 서비스 프록시, Spring Data 저장소, Hibernate, PostgreSQL을 실행했습니다. 외부 관측 연결의 autocommit/READ_COMMITTED, 단계별 값·버전, 예외 원인의 SQLSTATE, 최종 post/outbox 행 수·제목·버전·slug와 트랜잭션 정리를 assert했습니다. 예상한 제약 오류도 최종 DB 복구까지 검증해야 PASS가 됩니다. DDL은 실습마다 새 임시 DB에 생성합니다.

별도 연결은 기본 DataSource에서 직접 얻습니다. TransactionAwareDataSourceProxy로 교체하면 같은 의미가 아니므로 주의하세요. 공개 코드의 DataSource 구성 전체가 실험 조건입니다.

미검증: 운영 배포 상태, 성능/부하, JDBC batching, 낙관적 잠금 경합, 다른 격리 수준/DB/OS, IDENTITY 이외 생성 전략, Kafka 전송. 성능 우열과 운영 장애 복구를 이 결과로 주장하지 않습니다.

추가 실행: 저장소 밖 `/private/tmp`에서 `Spring-02.command`에 Enter를 전달하여 안내 모드의 여섯 PASS와 종료 코드 0을 확인했습니다. 자동 검증과 같은 버전·결과입니다.
