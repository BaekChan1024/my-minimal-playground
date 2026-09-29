# Spring 04 — 조회 일관성·격리 수준·JPA 1차 캐시

상세 글: [Spring 연재](https://blog.baekchan.com/category/spring)

JDK21과 Git이 필요합니다. Spring7.0.2 / Hibernate7.2.0.Final / PostgreSQL18.4를 실제 실행합니다. Docker나 기존 DB는 필요 없습니다. 첫 실행 시 Maven Central에서 의존성과 임시 PostgreSQL 바이너리를 받습니다. loopback 임의 포트로만 열고 종료 시 정리합니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach spring-tx-04-v1
./play-spring-04
```

Mac에서는 `Spring-04.command`를 더블클릭해도 됩니다. Enter로 시작, q로 서버 시작 전 종료합니다. 자동 검증은 `./play-spring-04 verify`. 설치된 JDK21을 찾아 사용하고 자동 설치하지 않습니다. 기존 저장소를 사용한다면 변경을 보존한 뒤 태그를 fetch하세요.

## 구성

`ReadLab.java`는 실제 JpaTransactionManager/TransactionTemplate과 같은 DataSource의 JDBC를 사용합니다. reader 트랜잭션이 열린 동안 직접 얻은 별도 연결의 writer를 커밋합니다. 단일 실행 스레드에서 두 연결의 명령을 순서대로 조정하므로 sleep이나 경쟁 타이밍에 의존하지 않습니다. backend PID가 다른지 검사합니다. 운영 설정·자격증명을 읽지 않습니다.

| 실험 | 비교 |
|---|---|
| jdbc-rc | READ COMMITTED에서 두 SELECT 사이 writer 커밋 |
| jdbc-rr | REPEATABLE READ에서 같은 순서 |
| jpa-rc | 같은 ID find, JDBC, JPQL scalar, refresh, clear+find |
| jpa-rr | 영속성 컨텍스트 재조회와 DB 스냅샷의 차이 |
| readonly-count-list-rc | DB readOnly=on에서 count 후 새 행 커밋, 목록 조회 |
| readonly-count-list-rr | 읽기 전용 + REPEATABLE READ의 count/목록 비교 |

초기값은 `id=1,title=original,version=0`. 수정 writer는 title=updated,version=1을 커밋합니다. 목록 실험의 writer는 id2 행을 추가합니다. 매 실험 뒤 reader 밖에서 writer 결과가 남는지도 확인합니다. JPA 실험에는 미반영 엔티티 변경이 없으며 2차 캐시나 쿼리 캐시를 설정하지 않습니다.

[예상 질문](EXERCISES.md) → 실행 → [답과 비교](ANSWERS.md) · [검증 기록](VERIFICATION.md)
