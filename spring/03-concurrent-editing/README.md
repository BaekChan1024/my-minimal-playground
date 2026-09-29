# Spring 03 — 동시 수정과 오래된 편집본

상세 글: [Spring 연재](https://blog.baekchan.com/post/같은-글을-두-사람이-수정하면-jpa-낙관적-잠금버전-충돌행-잠금-실습)

JDK 21과 Git이 필요합니다. Spring Framework 7.0.2 / Hibernate 7.2.0.Final / 임시 PostgreSQL 18.4를 실행합니다. Docker와 기존 DB 접속 정보는 필요 없습니다. 최초 실행 때 Maven Central에서 의존성과 PostgreSQL 바이너리를 받습니다. loopback 임의 포트만 사용하고 종료 시 정리합니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach spring-tx-03-v1
./play-spring-03
```

기존 저장소를 사용한다면 작업 중인 변경을 보존한 뒤 태그를 fetch하세요. Mac에서는 `Spring-03.command`를 더블클릭할 수 있습니다. Enter로 진행, q로 서버 시작 전에 종료합니다. `./play-spring-03 verify`는 자동 검증입니다. 설치된 JDK 21을 찾으며 시스템에 자동 설치하지 않습니다.

## 관측 구조

`LockingLab.java`가 `TransactionTemplate`과 `JpaTransactionManager`로 실제 트랜잭션을 만듭니다. Spring의 공유 EntityManager 프록시는 각 스레드의 트랜잭션에 연결되므로 하나의 실제 EntityManager를 두 스레드에서 공유하지 않습니다. 겹치는 수정은 두 worker와 latch로 읽기 순서 및 커밋 순서를 고정합니다. 별도 PG backend PID로 연결 분리도 확인합니다.

| 시나리오 | 질문 |
|---|---|
| overlap-unversioned | 버전 조건이 없으면 A 수정 후 B가 이전 상태를 기준으로 덮어쓰는가? |
| overlap-versioned | 둘 다 version=0을 읽은 경우 한쪽 flush가 거절되는가? |
| stale-request-version-only | B 요청이 새 트랜잭션에서 최신 엔티티를 읽으면 @Version만으로 오래된 편집본을 알 수 있는가? |
| stale-request-check | 클라이언트 expectedVersion 비교로 오래된 요청을 거절하는가? |
| row-lock-only | B가 DB 잠금을 기다린 뒤 최신 버전을 읽어도 낡은 사용자 입력을 적용할 수 있는가? |
| row-lock-check | 잠금 아래에서도 expectedVersion 검증이 필요한가? |

행 잠금은 시간 지연만으로 추정하지 않습니다. 별도 연결의 `pg_blocking_pids(B)`가 A를 포함하는 것을 확인한 다음 A를 커밋시킵니다. 대기 중 일반 SELECT가 기존 커밋 값을 읽는지도 확인합니다. 성공한 버전 수정마다 관측용 `outbox_record`에 작성자와 갱신 버전을 기록합니다. 이 테이블은 전송 파이프라인이 아니며 Kafka·릴레이·Inbox를 실행하지 않습니다.

[예상 질문](EXERCISES.md) → 실행 → [결과 해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
