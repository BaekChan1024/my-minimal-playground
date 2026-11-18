# 결과 해설

| 실험 | 관측 |
|---|---|
| flags | readOnly=true: Spring 동기화 readOnly=true, Hibernate flush MANUAL, 세션 defaultReadOnly=true, JDBC `isReadOnly()`=true, PG `transaction_read_only`=on. false면 모두 반대. 풀 크기 1에서 같은 연결이 다음 트랜잭션에 off로 돌아옴 |
| dirty-check | readOnly 엔티티 수정: 예외 없음, flush 0, UPDATE 0, DB original. 명시적 flush도 UPDATE 0. 쓰기 트랜잭션은 flush 1, UPDATE 1 |
| writes | JDBC UPDATE·JPQL 벌크 UPDATE·persist+flush는 SQLSTATE 25006으로 거부. persist만 하면 INSERT 0, 예외 없이 사라짐 |
| propagation | 쓰기→안쪽 readOnly: 안쪽도 PG off·flush AUTO, 쓰기 커밋. readOnly→안쪽 쓰기: 안쪽도 PG on·flush MANUAL, 엔티티 변경 소실, SQL은 25006. validateExistingTransaction=true면 안쪽 실행 전 IllegalTransactionStateException. REQUIRES_NEW는 다른 연결·PG off로 저장 |
| routing | 라우팅만: readOnly여도 primary(키 [primary]). Lazy 프록시+라우팅: replica. `setReadOnlyDataSource`: replica이지만 PG off. 읽기 풀을 readOnly로 구성하면 on |
| driver | `readOnlyMode=ignore`: JDBC isReadOnly=true지만 PG off, readOnly 트랜잭션의 UPDATE가 커밋됨 |
| open-in-view | 공유 EntityManager: readOnly 트랜잭션이어도 defaultReadOnly·엔티티 readOnly=false(flush MANUAL은 적용). 그 안의 변경이 같은 요청의 다음 쓰기 트랜잭션 커밋에서 UPDATE 1로 저장 |
| cost | 2만 개 로딩: readOnly는 스냅샷 0·커밋 flush 0. 로딩 중 할당량은 같고, GC 후 유지 힙과 커밋 시간이 작음 |

readOnly는 하나의 스위치가 아니라 Spring이 여러 층에 전달하는 힌트입니다. Hibernate는 flush를 끄고(MANUAL) 트랜잭션이 직접 만든 세션이면 엔티티를 읽기 전용으로 읽어 스냅샷을 두지 않습니다. JDBC 연결은 `setReadOnly(true)`가 되고, pgjdbc 기본 설정에서는 PostgreSQL 읽기 전용 트랜잭션이 됩니다. 그래서 DB까지 가는 SQL은 거부되지만, flush되지 않는 엔티티 변경은 예외 없이 사라집니다.

트랜잭션에 참여하는(REQUIRED) 안쪽 메서드의 readOnly는 바깥 트랜잭션의 속성을 바꾸지 않습니다. 라우팅은 연결을 얻는 시점이 핵심이고, DB 수준의 거부는 드라이버·풀·replica 설정에 따라 달라집니다.
