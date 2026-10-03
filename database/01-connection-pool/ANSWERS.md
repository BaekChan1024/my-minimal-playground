# 실행 후 해설

1. 세 번째 작업은 `getConnection()`에서 기다립니다. 풀 active는 빌려 간 연결이고, DB idle은 다음 명령을 기다리는 세션입니다. 연결이 반환되지 않았다면 둘은 동시에 성립합니다.
2. 이 합성 부하에서 풀 크기를 늘리면 동시에 연결을 보유할 작업 수가 늘어납니다. 반환을 앞당기면 DB 밖 작업 동안에도 다른 작업이 연결을 사용할 수 있습니다. 같은 200ms 작업은 남아 있지만 연결 점유는 줄어듭니다.
3. 첫 연결은 열린 트랜잭션 안에서 idle, 두 번째는 DB의 잠금 대기, 세 번째는 풀의 획득 대기입니다. 첫 트랜잭션 롤백 → 두 번째 UPDATE 완료 및 반환 → 세 번째 연결 획득 순서로 풀립니다.
4. PostgreSQL statement 제한은 서버에 명령이 도착한 뒤 적용됩니다. SQL 이전의 풀 대기는 Hikari connectionTimeout의 범위입니다. 이미 실행 중인 SQL을 connectionTimeout이 중단하지도 않습니다.

`pg_sleep`은 `Timeout/PgSleep`, 행 UPDATE 충돌은 이 실행에서 `Lock/transactionid`로 관측됐습니다. 기다리는 쿼리도 PostgreSQL state가 active일 수 있습니다.

## timeout 결과

- pool: SQLTransientConnectionException, SQL 시작 플래그 false
- statement: SQLState 57014, 서버에 보낸 pg_sleep 취소
- lock: SQLState 55P03, 잠금 획득 대기 제한 도달

시간은 소수점까지 일치할 필요가 없습니다. 로컬 임시 DB를 사용하고 관측 및 스케줄링 비용도 포함합니다.

## 선택 문제

풀 1에서 연결을 가진 채 200ms 작업을 하면 대략 8번의 작업이 직렬화됩니다. 실제 수치는 직접 확인해야 하며 이 조건은 기본 검증 기록에 포함하지 않았습니다.
잠금 실험의 statement 제한을 100ms로 줄이면 300ms lock 제한보다 먼저 57014로 취소될 것으로 예상합니다. 기본 코드의 55P03 검증은 의도적으로 실패할 것입니다. 이 수정 조건은 별도로 실행하지 않았습니다.

풀을 늘리면 항상 빨라진다는 결론은 낼 수 없습니다. DB 자원 경합이 거의 없는 SELECT 1과 인위적 대기만 비교했습니다.
읽기·쓰기를 하나의 트랜잭션으로 묶어야 한다면 정합성을 유지하며 작업 경계를 설계해야 합니다.
