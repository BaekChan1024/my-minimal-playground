# 실행 후 해설

| 조건 | 글/Outbox/시도 기록 | 외부 예외 |
|---|---|---|
| external-runtime | 0/0/0 | Fault |
| checked-default | 1/1/0 | IOException |
| checked-rollbackFor | 0/0/0 | IOException |
| self-invocation | 1/1/0 | Fault |
| caught-local | 1/0/0 | 없음 |
| caught-required | 0/0/0 | UnexpectedRollbackException |
| requires-new-audit | 0/0/1 | Fault |

- 자기 호출 조건은 바깥 메서드에 트랜잭션이 없습니다. 내부 this 호출은 새 프록시 경계를 지나지 않아 active=false이고 각 JDBC INSERT가 자동 커밋됩니다. 바깥 트랜잭션이 이미 있는 모든 상황까지 같은 결과로 일반화하면 안 됩니다.
- checked exception의 기본 규칙과 rollbackFor를 비교합니다. 전역 롤백 기본값을 바꾼 환경은 실습 전제와 다릅니다.
- caught-local은 같은 메서드 안의 Java 예외를 삼켰고 rollback-only를 설정하지 않았습니다. DB 자체를 실패 상태로 만드는 SQL 오류 실험이 아닙니다.
- caught-required는 안쪽 Bean의 프록시가 예외를 보고 공유 DB 트랜잭션을 rollback-only로 표시합니다. 바깥 catch가 그 상태를 취소하지 않아 최종 커밋 경계에서 실패합니다.
- DB의 pg_current_xact_id()를 비교해 REQUIRED의 같은 트랜잭션, REQUIRES_NEW의 다른 트랜잭션과 바깥 재개를 확인합니다. 안쪽 시도 기록은 독립 커밋이므로 바깥 실패 뒤에도 남습니다. 글 성공 기록을 의미하지 않습니다.
- Kafka·JPA flush·SQL 오류 복구·풀 고갈·NESTED·비동기 처리는 실행하지 않았습니다.
