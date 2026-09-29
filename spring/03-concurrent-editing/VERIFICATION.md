# 검증 기록

2026-09-29, macOS ARM64, JDK21. Spring Boot4.0.1 BOM과 gradle.lockfile로 의존성 고정.

`JAVA_HOME=$(/usr/libexec/java_home -v 21) ./gradlew :spring:03-concurrent-editing:verifyLab --write-locks --console=plain` 성공(exit0).

```text
Spring=7.0.2 Hibernate=7.2.0.Final PostgreSQL=18.4 JDBC=42.7.10 isolation=READ_COMMITTED
PASS overlap-unversioned A=committed B=committed title=B separateConnections=true
SQL update plain_post set title=? where id=?
PASS overlap-versioned A=committed B=OptimisticLockException row=A:1 outbox=A:1 separateConnections=true
SQL update versioned_post set title=?,version=? where id=? and version=?
PASS stale-request-version-only expectedB=0 B=committed row=B:2 outbox=A:1,B:2
PASS stale-request-check expectedB=0 B=EditConflict row=A:1 outbox=A:1
PASS row-lock-only blockedByA=true observerDuring=original:0 BloadedVersion=1 B=committed row=B:2 outbox=A:1,B:2
PASS row-lock-check blockedByA=true observerDuring=original:0 BloadedVersion=1 B=EditConflict row=A:1 outbox=A:1
ALL 6 SCENARIOS PASSED. 결과 비교: spring/03-concurrent-editing/ANSWERS.md
```

겹치는 두 트랜잭션은 서로 다른 PG backend PID인지 assert합니다. CountDownLatch로 둘 다 읽은 후 A 커밋, B flush 순서를 고정합니다. 잠금 실험은 pg_blocking_pids로 B가 A에게 실제로 막혔는지 확인해야 PASS입니다. 단순 sleep 경과를 증거로 사용하지 않습니다. 타임아웃과 finally의 gate 해제/worker 종료가 있습니다.

관측 연결의 autocommit/READ_COMMITTED, 최종 제목/version, actor별 Outbox 버전, 기대 예외와 Hibernate UPDATE의 version 조건을 대조합니다. 동시 worker에 실제 EntityManager를 직접 공유하지 않으며 Spring 공유 프록시가 스레드별 트랜잭션 컨텍스트를 사용합니다.

추가 검증: 저장소 밖 /private/tmp에서 Spring-03.command 안내 모드에 Enter 입력 → 동일 여섯 PASS, exit0.

미검증: HTTP 실제 호출/UI 충돌 처리, 운영 배포, DB 복제, 교착상태/부하, 모든 DB와 격리 수준, bulk update와 native UPDATE 우회, 탈착 엔티티 merge, Kafka·릴레이·Inbox·이벤트 재정렬. 잠금 실험은 최초 조회에 잠금을 지정하며 로컬 앱의 find→lock→refresh 경로 전체를 복제하지 않습니다.
