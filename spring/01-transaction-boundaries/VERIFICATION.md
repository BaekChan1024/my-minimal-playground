# 검증 — 2026-09-29

macOS ARM64 / JDK21 / Gradle9.2.1. Spring Boot4.0.1 BOM, 실제 Spring7.0.2, PostgreSQL18.4, JDBC driver42.7.10. embedded-postgres2.2.2, 바이너리 BOM18.4.0. 의존성은 gradle.lockfile에 고정했습니다.

실제 PostgreSQL 프로세스와 Spring AOP 프록시를 사용합니다. DataSourceTransactionManager/JdbcTemplate이며 JPA·Boot 웹 서버는 사용하지 않습니다. 실행기 자체에 트랜잭션이 없는지 확인하고, 각 서비스 호출 후 외부에서 커밋된 행 수를 조회합니다. REQUIRED/REQUIRES_NEW는 PostgreSQL 트랜잭션 식별자를 비교합니다.

```text
VERSIONS Spring=7.0.2 PostgreSQL=18.4
PASS external-runtime posts=0 outbox=0 audit=0 error=Fault active=true
PASS checked-default posts=1 outbox=1 audit=0 error=IOException active=true
PASS checked-rollbackFor posts=0 outbox=0 audit=0 error=IOException active=true
PASS self-invocation posts=1 outbox=1 audit=0 error=Fault active=false
PASS caught-local posts=1 outbox=0 audit=0 error=none active=true
PASS caught-required posts=0 outbox=0 audit=0 error=UnexpectedRollbackException sameTx=true
PASS requires-new-audit posts=0 outbox=0 audit=1 error=Fault differentTx=true outerResumed=true
PASS scenarios=7 proxyVerified=true committedRowsCheckedOutsideTransaction=true
```

자동 verify 종료0. 저장소 밖에서 Spring-01.command를 안내 모드로 실행한 결과도 동일하며 종료0입니다. 최초 Bean 등록 메서드 참조의 오버로드 모호성을 명시적인 무인자 lambda로 수정한 뒤 전체 재검증했습니다.

미검증: 실제 서비스 배포·JPA flush·SQL 오류 후 복구·커넥션 풀 고갈·Kafka·분산 원자성·NESTED·비동기·리액티브. Fault/IOException은 의도적으로 던진 Java 예외입니다.

공식 근거: [Spring @Transactional](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html), [롤백 규칙](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/rolling-back.html), [전파](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/tx-propagation.html). 참고 문서는 열람 당시7.0.x이고 실행 버전은 위 출력과 잠금 파일 기준입니다.
