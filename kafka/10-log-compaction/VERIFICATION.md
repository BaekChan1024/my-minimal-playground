# 검증 — 2026-09-29

macOS ARM64 / JDK21 / Gradle9.2.1 / Kafka4.1.1 / metadata4.1-IV1. 실제 임시 브로커1·컨트롤러1. min.compaction.lag.ms=3600000→0, delete.retention.ms=3600000 유지. 600KB 패딩과 별도 roll key로 세그먼트를 닫음. 최대60초 조건 조회.

```text
PASS before businessRecords=4 offsets=0,1,2,3 state=A:new tombstone=true
PASS after businessRecords=2 offsets=2,3 state=A:new tombstoneRetained=true
PASS emptyString BExists=true BValueLength=0
```

자동 verify와 저장소 밖 Mac 안내 실행 종료0. 실제 Cleaner의 과거 값 제거 및 Consumer 재읽기 검증. tombstone 시간 만료·디스크 절감량·성능·운영 변경은 미검증.
