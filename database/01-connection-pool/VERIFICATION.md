# 검증 기록

2026-10-03, macOS ARM64. Java·DB·드라이버·풀 버전은 아래 실행 출력과 Gradle lockfile로 확인했습니다.

- `./play-database-01 verify`: 종료 코드 0, 일곱 시나리오 통과.
- 저장소 밖에서 `Database-01.command`를 절대 경로로 호출하고 Enter 입력: 종료 코드 0, 예상 질문과 동일한 실험 완료.
- 시간은 실제 한 번의 자동 검증 출력입니다. 임계값이나 운영 권장값이 아닙니다.

```text
Java=21.0.6 PostgreSQL=18.4 JDBC=42.7.10 HikariCP=7.0.2 (Gradle lock)
PASS held-idle active=2 idle=0 pending=1 postgres=[idle,idle] waitingSqlStarted=false acquireMs=315.67 sqlMs=0.75
PASS returned-before-work thirdFinished=true otherWorkFinished=false acquireMs=0.02 sqlMs=0.34
PASS slow-sql active=1 idle=1 pending=0 postgres=Activity[state=active, waitType=Timeout, waitEvent=PgSleep, blockers=0] acquireMs=0.04 sqlMs=802.10
PASS lock-cascade active=2 idle=0 pending=1 blocker=idle-in-transaction blocked=Activity[state=active, waitType=Lock, waitEvent=transactionid, blockers=1] thirdSqlStarted=false blockedSqlMs=326.76 thirdAcquireMs=308.04
PASS pool-timeout limitMs=500 elapsedMs=513.42 exception=SQLTransientConnectionException sqlStarted=false
PASS statement-timeout limitMs=300 elapsedMs=304.85 sqlState=57014
PASS lock-timeout limitMs=300 elapsedMs=305.46 sqlState=55P03
PASS batch workers=8 pool=2 workMs=200 holdDuringWork=true totalMs=823.33 meanAcquireMs=308.52 meanHoldMs=205.29 completed=8
PASS batch workers=8 pool=4 workMs=200 holdDuringWork=true totalMs=415.83 meanAcquireMs=102.37 meanHoldMs=207.52 completed=8
PASS batch workers=8 pool=2 workMs=200 holdDuringWork=false totalMs=206.95 meanAcquireMs=0.84 meanHoldMs=0.41 completed=8
ALL 7 SCENARIOS PASSED. Timings are observations, not performance thresholds.
```

## 확인 범위

커넥션 획득 전 SQL 미실행, DB idle/잠금 상태, 후속 작업이 끝나기 전 다른 조회 완료, 세 timeout의 예외, 8개 작업의 완료를 검사합니다.
별도 관측 연결과 Hikari 풀의 연결을 구분합니다. pg_sleep은 SQL 지연을 만드는 장치이고 실제 CPU 병목이 아닙니다.
상태 확인은 최대 5초, 작업 Future 조회도 5초로 제한합니다. 풀 timeout은 일반 실험 3초, 획득 timeout 실험 500ms입니다.

## 한계

HTTP/API 서버를 띄우지 않았습니다. 운영 부하·네트워크 장애·CPU/디스크 포화·분위수·적정 풀 크기·커넥션 누수는 검증하지 않았습니다.
200/300ms는 의도한 애플리케이션 대기이며 실제 원격 호출이 아닙니다. `holdMs`는 반환 호출 직전까지이므로 close 비용은 제외합니다.
풀 지표를 원자적인 스냅샷으로 읽지 않습니다. 이 실험은 연결 보유/잠금 상태를 유지해 관측하지만 운영 순간값에는 변동이 있습니다.
Windows/Linux의 명령은 제공하지만 실행 확인은 macOS ARM64에서만 했습니다. 선택 문제의 수정 조건은 실행하지 않았습니다.

## 공식 근거

- [HikariCP 7.0.2](https://github.com/brettwooldridge/HikariCP/tree/HikariCP-7.0.2)
- [PostgreSQL 18 활동 상태](https://www.postgresql.org/docs/18/monitoring-stats.html)
- [PostgreSQL 18 timeout](https://www.postgresql.org/docs/18/runtime-config-client.html)
