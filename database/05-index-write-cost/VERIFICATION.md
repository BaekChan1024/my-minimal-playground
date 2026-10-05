# Database 05 검증

2026-10-05, macOS ARM64, Java 21.0.6, PostgreSQL 18.4, JDBC 42.7.10, block_size 8192.
자동 검증과 저장소 밖에서 `Database-05.command` 안내 모드를 Enter로 실행한 검증 모두 여섯 체크포인트 통과·종료 코드 0입니다.
아래 핵심 수치는 두 실행에서 같았습니다. 정확한 수치를 모든 환경에 보장하지 않습니다.

| INSERT 인덱스 수 | WAL bytes | records | FPI | heap bytes | index bytes |
|---:|---:|---:|---:|---:|---:|
| 0 | 1998241 | 10001 | 1 | 1744896 | 0 |
| 1 | 2672568 | 20032 | 4 | 1744896 | 245760 |
| 3 | 4094400 | 40099 | 4 | 1744896 | 827392 |

id=4242 SELECT는 같은 payload 한 행이며, 최상위 hit+read 합계 213 / 3 / 3회, 계획은 Seq Scan / Index Scan / Index Scan입니다.

| UPDATE | updated | HOT | new page | WAL bytes | FPI | heap before→after | index before→after |
|---|---:|---:|---:|---:|---:|---|---|
| hot50 | 10000 | 10000 | 0 | 2512687 | 435 | 3563520→3563520 | 245760→245760 |
| indexed50 | 10000 | 0 | 0 | 4445465 | 493 | 3563520→3563520 | 491520→933888 |
| dense100 | 10000 | 0 | 10000 | 5437311 | 245 | 1744896→3489792 | 245760→466944 |

`EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON)`의 최상위 Plan을 읽습니다. 부모·자식 수치를 중복 합산하지 않습니다.
UPDATE 반환 행 수가 아니라 현재 트랜잭션 카운터와 갱신된 실제 값으로 1만 행을 확인합니다.
heap은 pg_relation_size의 main fork, 인덱스는 pg_indexes_size이며 전체 DB 파일 사용량이 아닙니다.
관측은 초기 적재와 첫 전체 UPDATE입니다. 반복 갱신·VACUUM 후 정상 상태·동시 부하·운영 응답 시간은 검증 범위 밖입니다.
