# 검증 기록

2026-10-03 macOS ARM64. 자동 verifyLab 및 저장소 밖에서 Enter 입력으로 실행한 Database-02.command 모두 종료 코드 0.
여섯 체크포인트가 실제 PostgreSQL 18.4에서 통과했습니다. 아래는 안내 실행의 출력이며 자동 검증의 수치와 동일했습니다.

```text
Java=21.0.6 PostgreSQL=18.4 JDBC=42.7.10 blockSize=8192
seed Size[rows=20000, heap=11706368, indexes=466944, total=12206080, dead=0, free=712072]
after-delete Size[rows=2000, heap=11706368, indexes=466944, total=12206080, dead=7, free=10500264]
PASS delete-visible-rows-without-shrinking-heap
after-vacuum Size[rows=2000, heap=11706368, indexes=466944, total=12214272, dead=0, free=10526928]
PASS vacuum-leaves-reusable-space-inside-existing-file
after-reinsert Size[rows=20000, heap=11706368, indexes=868352, total=12615680, dead=0, free=712072]
PASS reinsert-reuses-heap-space
before-full Size[rows=3800, heap=11706368, indexes=868352, total=12615680, dead=0, free=9539728]
after-full Size[rows=3800, heap=2228224, indexes=106496, total=2342912, dead=0, free=139296]
PASS full-rewrites-and-shrinks filenodeChanged=true
snapshot-open-after-vacuum Size[rows=1000, heap=5857280, indexes=245760, total=6144000, dead=9000, free=360120]
snapshot readers: old=10000 new=1000
snapshot-closed-after-vacuum Size[rows=1000, heap=5857280, indexes=245760, total=6144000, dead=0, free=5267560]
PASS old-snapshot-delays-reclamation
PASS vacuum-outside-transaction sqlState=25001
ALL 6 CHECKPOINTS PASSED. Byte counts describe this local run, not disk-capacity guarantees.
```

## 검사한 내용

행 수 감소와 heap 크기 유지, 정리 후 빈 공간, 같은 크기 재삽입 시 heap 재사용, FULL의 행 수 보존·크기 감소·filenode 변경,
오래된 스냅샷의 가시성·종료 후 정리, 트랜잭션 안 VACUUM의 25001 거절을 확인했습니다.

첫 검증에서는 VACUUM 이전 공간 정리를 고려하지 않아 “VACUUM만으로 절반 이상 새 빈 공간이 생긴다”는 잘못된 조건이 실패했습니다.
삭제 후 관측 접근에도 페이지 정리가 포함될 수 있음을 반영하여, 최초 적재 대비 정리 후 공간과 실제 재삽입을 검사하도록 고쳤습니다.
실측을 예상에 맞추기 위해 변경하지 않았습니다. 각 관측 쿼리의 개별 기여까지 분리 측정한 것은 아닙니다.

## 조건과 한계

- 512바이트 payload, 기본 키 인덱스, 8KiB 페이지. 고정 테이블명만 사용합니다.
- 실습 테이블의 일반 autovacuum을 제외하며 일반 VACUUM은 TRUNCATE FALSE입니다. 운영 설정을 바꾸지 않습니다.
- pgstattuple 전체 스캔은 페이지 상태를 누적 관측합니다. dead 분류가 즉시 제거 가능하다는 뜻은 아닙니다.
- 공간 재사용 검증은 같은 payload 크기로 18,000행을 다시 넣는 조건이며 1페이지 이내의 heap 증가만 허용합니다. 실제로는 증가가 없었습니다.
- FULL의 최대 추가 디스크 사용량·동시 접근 차단 시간은 측정하지 않았습니다. 해당 제약은 공식 문서에 근거합니다.
- autovacuum 실행 주기·WAL 증가·운영 부하·서버 여유 디스크·Windows/Linux는 미검증입니다.
- 선택 문제의 수정 조건은 실행하지 않았습니다. 사용자 숙지는 미확인입니다.

## 공식 자료

- [정기 VACUUM](https://www.postgresql.org/docs/18/routine-vacuuming.html)
- [VACUUM 명령과 옵션](https://www.postgresql.org/docs/18/sql-vacuum.html)
- [크기 함수](https://www.postgresql.org/docs/18/functions-admin.html)
- [pgstattuple](https://www.postgresql.org/docs/18/pgstattuple.html)
