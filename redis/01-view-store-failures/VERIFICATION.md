# 검증 — 2026-11-22

macOS ARM64(Apple M1 Pro) / OpenJDK 21.0.6 / Gradle 9.2.1 / Redis 8.10.3(Homebrew, 로컬 `redis-server`) / Lettuce 6.8.1 / PostgreSQL 18.4(zonky embedded-postgres 2.2.2) / HikariCP 7.0.2.
Redis는 실험마다 새 임시 디렉터리·임의 포트로 띄우고, 기본 사용자에 `+@all -@dangerous -@admin` ACL을 겁니다. 앱 연결은 명령 타임아웃 300ms, 연결·핸드셰이크 1초입니다. redis 방식의 조회 기록은 운영처럼 한 트랜잭션 안에서 `SET NX` → UPDATE → `ZINCRBY` → `EXPIRE` → COMMIT입니다.
지연은 루프백 TCP 프록시가 넣습니다(덩어리마다 받은 시각 + 지연에 전달).

## 자동 검증 출력 (1회)

```text
PASS outage client=rejectWhileDisconnected requestsWhileDown=82 uncounted=82 latencyWhileDown p50=0.3ms max=2.5ms firstCountedAfterRestart=177ms uncountedAfterRestart=15 firstRequestsAfterRestart=[U0, U0, U0, U0, U0] counted=93 postgres=93
PASS outage client=lettuceDefault requestsWhileDown=3 uncounted=3 latencyWhileDown p50=305.4ms max=308.0ms firstCountedAfterRestart=522ms uncountedAfterRestart=1 firstRequestsAfterRestart=[U305, F207, F1, F1, F1] counted=97 postgres=97
PASS slow responseDelay=0ms first: counted=20 timedOut=0 p50=0.9ms max=2.2ms | again without delay: repeat=20 counted=0 | neverCounted=0
PASS slow responseDelay=100ms first: counted=20 timedOut=0 p50=314.4ms max=332.1ms | again without delay: repeat=20 counted=0 | neverCounted=0
PASS slow responseDelay=250ms first: counted=20 timedOut=0 p50=765.2ms max=775.5ms | again without delay: repeat=20 counted=0 | neverCounted=0
PASS slow responseDelay=400ms first: counted=0 timedOut=20 p50=303.0ms max=305.8ms | again without delay: repeat=20 counted=0 | neverCounted=20
PASS restart aof=no writeP50=0.31ms writeP99=0.61ms keysAfterKill=0/2000 rankingAfterKill=0 countedAgain=2000 postgres=4000
PASS restart aof=everysec writeP50=0.26ms writeP99=0.50ms keysAfterKill=2000/2000 rankingAfterKill=2000 countedAgain=0 postgres=2000
PASS restart aof=always writeP50=12.85ms writeP99=24.74ms keysAfterKill=2000/2000 rankingAfterKill=2000 countedAgain=0 postgres=2000
PASS memory policy=allkeys-lru firstPass: counted=30000 refused=0 evicted=23425 viewKeysLeft=6416 rankingKey=kept rankingSum=30001 | replay first 5000: countedAgain=5000 refused=0
PASS memory policy=volatile-ttl firstPass: counted=30000 refused=0 evicted=23432 viewKeysLeft=6404 rankingKey=kept rankingSum=30001 | replay first 5000: countedAgain=5000 refused=0
PASS memory policy=noeviction firstPass: counted=6944 refused=23056 evicted=0 viewKeysLeft=6945 rankingKey=kept rankingSum=6945 | replay first 5000: countedAgain=0 refused=5000
     roundTripsPerView {POSTGRES=redis 0 + postgres 3, REDIS=redis 3 + postgres 2, REDIS_SCRIPT=redis 1 + postgres 2}
PASS network rtt=0ms recordP50 postgres=0.20ms redis=0.29ms redisScript=0.21ms
PASS network rtt=1ms recordP50 postgres=4.77ms redis=8.39ms redisScript=5.31ms
PASS network rtt=5ms recordP50 postgres=20.94ms redis=33.97ms redisScript=20.49ms
PASS experiments=5 realRedis=8.10.3 realPostgres=true
```

`verify` 5회(그중 2회는 새로 복제한 디렉터리에서 `./play-redis-02 verify`: 실습 파일이 같은 직전 커밋과, 다른 실습이 합쳐진 뒤의 이 커밋), 안내 실행(모든 Enter) 1회, 모두 종료 코드 0. 안내 실행 중 2번 질문에서 q를 입력하면 임시 Redis·DB를 정리하고 종료 코드 0으로 끝납니다. 잘못된 인자는 사용법과 종료 코드 2. 실행 뒤 남은 `redis-server` 프로세스와 임시 디렉터리 없음.

실행마다 같았던 값: 판정 대상 전부. 실험 1의 미집계 = 끊긴 동안의 요청 수, PostgreSQL 조회수 = 센 수. 실험 2의 집계·반복·영구 미집계 수. 실험 3의 남은 키·순위·다시 센 수. 실험 4의 `volatile-ttl` 순위 합 30,001과 `noeviction`의 다시 센 수 0. 실험 5의 조회당 왕복 수.

달랐던 값(판정하지 않음):

| 값 | 범위 |
|---|---|
| 실험 1 즉시 거부: 끊긴 동안 요청 수·p50 / 재시작 뒤 첫 집계까지 / 그 사이 미집계 | 82~88·0.1~0.3ms / 166~182ms / 14~15 |
| 실험 1 Lettuce 기본값: 재시작 뒤 첫 집계까지 | 522~539ms (재시작 순간의 요청은 300~306ms 기다린 뒤 미집계, 다음 요청은 205~227ms 기다린 뒤 집계. 6회 모두 같은 양상) |
| 실험 3 `always` 쓰기 p50 | 11.07~13.89ms |
| 실험 4 `allkeys-lru` 순위 합 | 9,445 / 30,001 × 5 (6회 중 1회는 순위 합이 줄었음) |
| 실험 4 퇴출 키 수 / `noeviction` 집계 수 | 23,408~23,437 / 6,934~6,952 |
| 실험 5 왕복 5ms의 p50(postgres / redis / script) | 17.93~20.94 / 29.90~33.97 / 18.43~21.16ms |

## 판정 기준

| 실험 | 판정 |
|---|---|
| 1 | 장애 전 모두 집계, 끊긴 동안의 요청 모두 미집계, 재시작 뒤 다시 집계(재시작을 시작한 뒤 보낸 요청은 모두 재시작 뒤로 셈), PostgreSQL 조회수 = 센 수, 끊긴 동안 순위 조회 불가. 즉시 거부: 끊긴 동안 최대 50ms 미만. 기본값: 끊긴 동안 p50 250ms 이상 |
| 2 | 300ms 미만 지연: 20개 모두 한 번 집계, 지연이 있으면 p50 ≥ 지연 × 2.8(명령 세 개). 400ms: 첫 조회 20개 모두 타임아웃, 지연 제거 뒤 20개 모두 반복 판정(영구 미집계 20), 최대 지연이 400ms 미만. PostgreSQL 합 60 |
| 3 | 2,000개 모두 집계. AOF 없음: 남은 키 0, 다시 센 수 2,000. `everysec`·`always`: 남은 키 2,000, 순위 2,000, 다시 센 수 0. PostgreSQL = 처음 + 다시 센 수 |
| 4 | `allkeys-lru`·`volatile-ttl`: 거부 0, 퇴출 > 0, 다시 센 수 > 0. `volatile-ttl`은 순위 합 = 집계 + 1(예열). `noeviction`: 거부 > 0, 퇴출 0, 다시 센 수 0 |
| 5 | 모든 조회 집계. 왕복 5ms에서 redis − postgres ≥ 왕복 × 1.4, \|script − postgres\| ≤ 왕복 × 0.8 |

## 바꿔 보기 확인

`Views.COMMAND_TIMEOUT`을 500ms로 바꾼 실행: 실험 1과 실험 2의 0·100·250ms 통과 후 `FAIL delay 400 every first view timed out`(종료 코드 1). 확인 후 되돌렸습니다.

## 검증하지 않은 범위

- 운영체제·전원이 꺼지는 경우(페이지 캐시 유실). `everysec`가 프로세스 종료에서 아무것도 잃지 않은 것은 관측이고, "응답 전에 `write()`, fsync는 1초마다"라는 설명은 관측과 맞는 추론입니다. Redis 문서는 `always`가 응답 전에 fsync한다고만 적고 있습니다.
- 여러 Redis 노드(복제·Sentinel·Cluster)와 장애 조치.
- 동시 요청이 많을 때의 기본값(쌓아 두기) 동작. 실험 1은 요청을 하나씩 보냈습니다.
- 서버 디스크의 fsync 비용. `always`의 12ms는 이 Mac의 값입니다.
- 블로그 운영 Redis(8.2.10)와 다른 버전. 사용한 명령과 정책은 두 버전에 모두 있습니다.
