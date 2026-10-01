# 2026-10-01 검증

환경: macOS ARM64, JDK 21, Gradle Wrapper 9.2.1. 실행 출력에서 Spring 7.0.2, PostgreSQL 18.4, JDBC 42.7.10을 확인했습니다. Spring Boot 4.0.1 BOM, Zonky Embedded PostgreSQL 2.2.2, PostgreSQL binaries BOM 18.4.0과 `gradle.lockfile`을 사용합니다. 이 실습은 Boot 서버를 띄우지 않습니다.

자동 검증:

```bash
./gradlew :operations:01-cutover-recovery:verifyLab --write-locks --console=plain
```

관측 출력:

```text
Spring=7.0.2 PostgreSQL=18.4 JDBC=42.7.10
PASS forward-import rows=2 normalizedValuesMatch=true
PASS stale-recovery countBoth=2 oldIds=[1, 2] currentIds=[1, 3] editLost=true deletedRowReturns=true newRowMissing=true
PASS fresh-reverse normalizedValuesMatch=true incompatibleFormatRejected=true unadjustedSequenceCollision=true nextWriteId=4
PASS object-recovery oldBackupMissing=2 corruptionDetected=true restoredReferences=2 sourceRowsUnchanged=true
ALL 4 RECOVERY CHECKPOINTS PASSED. See ANSWERS.md for comparison and limits.
```

BUILD SUCCESSFUL. 저장소 밖 임시 디렉터리에서 `Recovery-01.command`를 절대 경로로 실행하고 Enter를 전달한 안내 경로도 같은 결과와 종료 코드 0을 확인했습니다.

검증하지 않은 것: 실제 pg_dump/pg_restore archive, 운영 DB/스토리지, 장애·트래픽 전환, 다중 writer의 정지, Kafka 이벤트 재생, 실제 MinIO API, 동시 파일 변경, 운영 복구 시간·RPO, 전체 스키마 호환성, Windows/Linux. 반복 실행 결과는 사용자 학습 완료를 뜻하지 않습니다.
