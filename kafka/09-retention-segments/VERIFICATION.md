# 検증 — 2026-09-29

macOS ARM64 / JDK21 / Gradle9.2.1 / Kafka4.1.1 / metadata4.1-IV1. 자동 verify와 저장소 밖 Mac 안내 실행 종료0. 약600KB 레코드9개, old timestamp=현재-1시간, fresh=현재, 1MiB 세그먼트, retention=-1→600000ms. 그룹 오프셋은 Admin으로0, Consumer는 assign/seek.

```text
PASS before start=0 end=9 records=9 multipleSegments=true committed=0
PASS after start=8 end=9 committed=0 oldSegmentFilesRemoved=true
PASS reset=none offset=0 error=OffsetOutOfRangeException
PASS reset=earliest offset=0 records=fresh
```

세그먼트 .log 및 삭제 대기 파일 수 감소를 관측. 백그라운드 정리 시작을 고려해 최대60초 상태 조회. 초기 작은 segment.bytes 설정 거절 및 짧은 관측 제한 실패는 수정 후 재검증했다. 자연 경과1시간, 크기 기반 retention, 원격 저장, subscribe 재가입, 성능은 미검증.
