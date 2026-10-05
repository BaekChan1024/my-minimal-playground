# Database 05 — 인덱스의 쓰기 비용과 HOT

[블로그 글: 인덱스는 많을수록 좋을까? — 조회 이득과 쓰기 비용](https://blog.baekchan.com/post/인덱스는-많을수록-좋을까-조회-이득과-쓰기-비용)

실제 임시 PostgreSQL에서 같은 1만 행의 INSERT·SELECT·UPDATE를 비교합니다.
JDK 21과 Git이 필요합니다. 첫 실행에는 의존성과 PostgreSQL 바이너리를 내려받을 네트워크가 필요합니다. Docker나 별도 DB 설치는 필요 없습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach database-05-v1
./play-database-05
```

기존 저장소는 변경을 보존한 뒤 `git fetch origin tag database-05-v1`로 태그를 가져오세요.
Mac은 `Database-05.command`로도 실행할 수 있습니다. 자동 검증은 `./play-database-05 verify`입니다.
안내 모드는 예상 질문을 보여 준 뒤 Enter로 시작합니다. SQL은 Java 실행 코드에서 확인하고 별도 복사본에서 조건을 바꿀 수 있습니다.

시작 전에 [문제](EXERCISES.md)를 읽고, 실행 후 [해설](ANSWERS.md)과 비교하세요.

## 확인하는 여섯 가지

1. 인덱스 0·1·3개 테이블에 삽입한 네 컬럼과 1만 행이 정확히 같습니다.
2. 이 데이터에서 추가 인덱스는 INSERT WAL과 할당 공간을 늘립니다.
3. id 하나의 조회는 인덱스 1개로 버퍼 접근이 줄고, 추가 두 인덱스로 더 줄지는 않습니다.
4. score 인덱스는 같은 페이지에 새 버전이 들어가도 score 변경의 HOT을 막습니다.
5. 비인덱스 컬럼을 바꾸더라도 같은 페이지 여유가 없으면 HOT이 보장되지 않습니다.
6. 낮은 fillfactor는 처음부터 더 큰 heap을 쓰며, 첫 갱신 후 파일 확장 여부도 달라집니다.

## 실험 조건과 범위

- 2026-10-05 macOS ARM64/JDK 21.0.6/PostgreSQL 18.4/pgjdbc 42.7.10 검증. 다른 OS의 실행 결과는 미확인입니다.
- 새 loopback 서버·임의 포트·임시 데이터를 사용하고 정상 종료 시 정리합니다. 외부 서버 접속 설정은 없습니다.
- shared_buffers 32MB, fsync/synchronous_commit/full_page_writes on, wal_level replica, wal_compression off.
- 각 측정 쓰기 전 임시 서버에서 CHECKPOINT. 자동 체크포인트 1h/1GB. 병렬 조회·JIT off, 테이블 autovacuum off.
- INSERT 비교는 fillfactor 100과 B-tree 0·1·3개. 인덱스 없는 비교군을 위해 PK/UNIQUE는 없습니다.
- UPDATE는 새 테이블에 같은 데이터를 넣고 VACUUM ANALYZE 후 전체 행을 한 번 바꿉니다. WHERE로 찾는 비용을 섞지 않습니다.
- 현재 트랜잭션 안에서 `pg_stat_xact_user_tables`를 읽고 실제 변경 결과를 확인한 뒤 커밋합니다.
- EXPLAIN ANALYZE는 쓰기를 실제 실행합니다. 실행 코드는 격리된 실습 전용입니다.
- [검증 결과](VERIFICATION.md)의 수치는 기준 실행의 관측입니다. WAL/FPI/공간은 환경에 따라 달라질 수 있고, 응답 시간 배수·처리량 보장이 아닙니다.

의존성은 이 디렉터리의 Gradle lockfile에 고정합니다. 고정 공개 버전은 `database-05-v1`입니다.
