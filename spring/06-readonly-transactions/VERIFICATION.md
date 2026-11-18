# 검증 기록

2026-11-18, macOS ARM64/JDK21. Boot4.0.1 BOM과 gradle.lockfile 사용.

`./play-spring-06 verify` exit0. 같은 날 네 번 실행해 판정 항목은 모두 같았고, cost의 시간 값만 아래 범위에서 달라졌습니다.

```text
Spring=7.0.2 Hibernate=7.2.0.Final PostgreSQL=18.4 JDBC=42.7.10 HikariCP=7.0.2
OBS flags readOnly=true  springSync=true hibernateFlush=MANUAL sessionDefaultReadOnly=true jdbcConnectionReadOnly=true pgTransactionReadOnly=on
OBS flags readOnly=false springSync=false hibernateFlush=AUTO sessionDefaultReadOnly=false jdbcConnectionReadOnly=false pgTransactionReadOnly=off
PASS flags samePhysicalConnection=true resetAfterReadOnly=true
OBS dirty-check readOnly=true entityReadOnly=true flushes=0 hibernateUpdates=0 exception=none dbTitle=original
OBS dirty-check readOnly=false entityReadOnly=false flushes=1 hibernateUpdates=1 exception=none dbTitle=changed
OBS dirty-check readOnly=true explicitFlush=true hibernateUpdates=0 exception=none dbTitle=original
PASS dirty-check readOnlyChangeSilentlyDropped=true
OBS writes jdbcUpdate=25006 UncategorizedSQLException: ERROR: cannot execute UPDATE in a read-only transaction
OBS writes jpqlBulkUpdate=25006 GenericJDBCException: ERROR: cannot execute UPDATE in a read-only transaction
OBS writes persistWithoutFlush exception=none hibernateInserts=0 rows=1
OBS writes persistAndFlush hibernateInserts=1 rows=1 exception=25006 GenericJDBCException: ERROR: cannot execute INSERT in a read-only transaction
PASS writes sqlReachingDbRejected=3 silentlyDropped=1
OBS propagation outer=RW inner=readOnly/REQUIRED innerPg=off innerSync=false innerFlush=AUTO innerWrite=committed
OBS propagation outer=readOnly inner=RW/REQUIRED innerPg=on innerFlush=MANUAL entityChange=dropped(exception=none) jdbcUpdate=25006 UncategorizedSQLException: ERROR: cannot execute UPDATE in a read-only transaction
OBS propagation validateExistingTransaction=true innerRan=false exception=IllegalTransactionStateException: Participating transaction with definition [PROPAGATION_REQUIRED,ISOLATION_DEFAULT,timeout_15] is not marked as read-only but existing transaction is
OBS propagation outer=readOnly inner=RW/REQUIRES_NEW otherConnection=true innerPg=off outerPgAfter=on dbTitle=requires-new
PASS propagation scenarios=4
OBS routing routing-only                             readOnlyTx=primary pgReadOnly=on routingKeys=[primary] readWriteTx=primary
OBS routing lazy+routing                             readOnlyTx=replica pgReadOnly=on routingKeys=[replica] readWriteTx=primary
OBS routing lazy+readOnlyDataSource                  readOnlyTx=replica pgReadOnly=off routingKeys=- readWriteTx=primary
OBS routing lazy+readOnlyDataSource(pool readOnly)   readOnlyTx=replica pgReadOnly=on routingKeys=- readWriteTx=primary
PASS routing eagerConnectionWentToPrimary=true lazyProxyReachedReplica=true readOnlyDataSourceNotMarkedReadOnly=true
OBS driver readOnlyMode=ignore jdbcConnectionReadOnly=true pgTransactionReadOnly=off jdbcUpdate=committed dbTitle=written-anyway
PASS driver readOnlyIsHint=true
OBS open-in-view sharedEntityManager=false readOnlyTx(flush=MANUAL defaultReadOnly=true entityReadOnly=true pg=on) afterReadOnlyTx=original laterWriteTxUpdates=0 afterLaterWriteTx=original
OBS open-in-view sharedEntityManager=true  readOnlyTx(flush=MANUAL defaultReadOnly=false entityReadOnly=false pg=on) afterReadOnlyTx=original laterWriteTxUpdates=1 afterLaterWriteTx=renamed-in-readonly
PASS open-in-view readOnlyChangeFlushedByLaterWriteTx=true
OBS cost readOnly=false rows=20000 snapshots=20000 readOnlyEntries=0 commitFlushes=1 loadAllocKiB(median)=10571 retainedKiB(median)=6875 commitMs(median)=3.94 min=3.44 max=9.81
OBS cost readOnly=true  rows=20000 snapshots=0 readOnlyEntries=20000 commitFlushes=0 loadAllocKiB(median)=10571 retainedKiB(median)=6451 commitMs(median)=0.97 min=0.71 max=1.25
PASS cost rounds=7 warmup=3 (timings are local observations, not asserted)
ALL 8 SCENARIOS PASSED. 비교 해설: spring/06-readonly-transactions/ANSWERS.md
```

판정 방식: 각 트랜잭션 안에서 `SHOW transaction_read_only`, JDBC `Connection.isReadOnly()`, Hibernate 세션의 flush 모드·defaultReadOnly·엔티티 readOnly, Spring `isCurrentTransactionReadOnly()`를 직접 읽어 assert합니다. Hibernate가 보낸 SQL만 StatementInspector로 셉니다(JdbcTemplate SQL은 제외). 매 실험 뒤 트랜잭션 밖에서 DB 값과 행 수를 다시 읽습니다. 쓰기 거부는 예외 체인의 SQLException SQLState 25006으로 확인합니다.

라우팅 원인 확인: Spring 7.0.2 `LazyConnectionDataSourceProxy`의 대상 연결 획득 코드는 `readOnly && readOnlyDataSource == null`일 때만 `DataSourceUtils.setReadOnlyIfPossible`을 호출합니다(바이트코드 확인). OSIV 원인 확인: `HibernateJpaDialect.beginTransaction`은 flush 모드는 항상 준비하지만 `setDefaultReadOnly(true)`는 `ResourceTransactionDefinition.isLocalResource()`일 때만 호출합니다.

cost 시간 범위(네 번 실행의 중앙값): 쓰기 트랜잭션 커밋 3.5~4.7ms, readOnly 커밋 0.8~1.1ms. 로딩 할당 10,571KiB는 양쪽 동일, GC 후 유지 힙 중앙값은 6,875KiB 대 6,451KiB. 작은 단일 테이블·로컬 임베디드 DB 기준이며 일반화하지 않습니다.

추가 실행: 저장소 밖 디렉터리에서 `Spring-06.command`에 Enter 전달, 여덟 PASS와 정상 종료 확인.

미검증: Spring Boot 애플리케이션·실제 HTTP 요청의 OSIV, 실제 스트리밍 복제와 복제 지연, MySQL 등 다른 DB와 드라이버, `DataSourceTransactionManager` 단독 구성, Spring Data JPA 리포지토리 기본 트랜잭션, 2차 캐시, 대용량·동시 부하에서의 성능, 다른 OS.
