# Spring 06 — readOnly 트랜잭션이 실제로 바꾸는 것

상세 글: [Spring 연재](https://blog.baekchan.com/post/transactionalreadonly-true는-무엇을-바꿀까-변경-감지쓰기-거부전파라우팅-실습)

JDK21과 Git이 필요합니다. Spring7.0.2 / Hibernate7.2.0.Final / PostgreSQL18.4 / HikariCP7.0.2를 실제 실행합니다. Docker나 기존 DB는 필요 없습니다. 첫 실행 시 Maven Central에서 의존성과 임시 PostgreSQL 바이너리를 받습니다. loopback 임의 포트로만 열고 종료 시 정리합니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach spring-tx-06-v1
./play-spring-06
```

Mac에서는 `Spring-06.command`를 더블클릭해도 됩니다. Enter로 시작, q로 서버 시작 전 종료합니다. 자동 검증은 `./play-spring-06 verify`. 설치된 JDK21을 찾아 사용하고 자동 설치하지 않습니다.

## 구성

`ReadOnlyLab.java`는 Spring Boot의 JPA 기본 구성과 같은 조합(JpaTransactionManager + Hibernate + HikariCP + JdbcTemplate)을 직접 만듭니다. Boot 애플리케이션이나 HTTP 서버는 띄우지 않습니다. 운영 설정·자격증명을 읽지 않습니다.

| 실험 | 비교 |
|---|---|
| flags | readOnly=true/false에서 Spring·Hibernate·JDBC·PostgreSQL 각 층의 값, 풀 연결 재사용 시 초기화 |
| dirty-check | readOnly에서 엔티티 수정·명시적 flush, 읽기·쓰기 트랜잭션과 비교 |
| writes | readOnly에서 JDBC UPDATE, JPQL 벌크 UPDATE, persist(flush 없음/있음) |
| propagation | 쓰기→안쪽 readOnly, readOnly→안쪽 쓰기(REQUIRED), validateExistingTransaction, REQUIRES_NEW |
| routing | readOnly 기반 primary/replica 라우팅 네 가지 구성 |
| driver | pgjdbc `readOnlyMode=ignore`에서 readOnly 트랜잭션의 쓰기 |
| open-in-view | 요청 범위 EntityManager를 공유할 때 readOnly 트랜잭션의 변경이 다음 쓰기 트랜잭션에서 저장되는지 |
| cost | 엔티티 2만 개 로딩 시 스냅샷·flush·할당·유지 힙·커밋 시간 |

초기값은 `post_record(id=1,title=original,version=0)`. routing의 replica는 같은 PostgreSQL 서버의 두 번째 데이터베이스이며 실제 복제가 아닙니다. open-in-view는 Boot의 `OpenEntityManagerInViewInterceptor`가 하는 것처럼 EntityManagerHolder를 직접 바인딩해 흉내 냅니다. cost의 시간 값은 로컬 관측이며 판정하지 않습니다.

[예상 질문](EXERCISES.md) → 실행 → [답과 비교](ANSWERS.md) · [검증 기록](VERIFICATION.md)
