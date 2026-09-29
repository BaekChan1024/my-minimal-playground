# 실행 결과와 해설

2026-09-29 macOS ARM64 실측. 아래 값은 같은 조건으로 실행한 결과이며 사용자 학습 완료를 뜻하지 않습니다.

| 시나리오 | 트랜잭션 내부 | 프록시 호출 종료 뒤 |
|---|---|---|
| identity-rollback | ID 할당, same=1, observer=0. flush 뒤도 observer=0 | posts=0 |
| saveAndFlush-commit | saveAndFlush 뒤 observer=0 | posts=1, title=new, version=0 |
| dirty-flush-rollback | same=old/version=0 → flush → same=new/version=1, observer=old:0 | title=old, version=0 |
| auto-query-rollback | JPQL count=1, same=new/version=1, observer=old:0 | title=old, version=0 |
| unique-at-flush | beforeFlush만 도달, SQLSTATE 23505 | 원래 두 slug 유지 |
| deferred-at-commit | 두 flush 성공, 본문 return 직전 도달 | SQLSTATE 23503, title=old/version=0, outbox=0 |

1. IDENTITY에서 ID 확보를 위한 INSERT가 실행돼도 서비스 트랜잭션은 열려 있습니다. 식별자 유무는 커밋의 증거가 아닙니다.
2. 저장소가 바깥 서비스 트랜잭션에 참여하므로 saveAndFlush가 독립적으로 커밋하지 않습니다. 외부 서비스 경계가 없으면 저장소 자체가 트랜잭션 경계가 될 수 있으므로 호출 구조를 함께 봐야 합니다.
3. 이번 엔티티는 같은 영속성 컨텍스트에서 관리됩니다. 변경 감지로 UPDATE가 실행되며 Hibernate가 버전을 증가시켰습니다. 롤백 후 DB는 0입니다. 탈착 엔티티의 setter만으로도 저장된다는 뜻은 아닙니다.
4. AUTO 모드에서 변경 테이블과 겹치는 JPQL을 사용했습니다. 모든 SELECT가 항상 flush한다는 실험이 아닙니다.
5. 새 IDENTITY INSERT 대신 기존 엔티티의 slug 변경을 사용해 오류가 명시적인 flush에 걸리도록 했습니다. 오류 후 같은 트랜잭션을 계속 사용하지 않습니다.
6. 실습에만 둔 DEFERRABLE INITIALLY DEFERRED FK는 트랜잭션 종료 시 검증됩니다. 메서드 본문의 return과 프록시 호출의 성공 반환은 다릅니다. flush 성공 뒤 외부 메시지 전송이 있었다면 DB 롤백으로 그 전송을 되돌릴 수 없습니다. Outbox의 원자성은 flush가 아니라 업무 행과 이벤트 행을 묶는 DB 트랜잭션에 있습니다.
