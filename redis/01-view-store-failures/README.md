# Redis 01 — 조회수 중복 제거가 Redis에 있을 때: 죽거나, 느리거나, 재시작하거나, 꽉 차거나

상세 글: 블로그 발행 후 링크를 추가합니다.

JDK 21, Git, 그리고 **로컬 `redis-server`(Redis 7 이상)**가 필요합니다. 다른 실습과 달리 Redis는 실습이 내려받지 않습니다. macOS에서는 `brew install redis`로 설치하거나 `REDIS_SERVER`에 실행 파일 경로를 지정하세요. PostgreSQL 18.4는 첫 실행 때 Maven Central에서 받아 임시로 띄웁니다. Docker는 필요 없습니다. macOS ARM64(Redis 8.10.3)에서 검증했으며 다른 OS·버전은 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach redis-02-v1
./play-redis-02
```

Mac에서는 `Redis-02.command`를 더블클릭해도 됩니다. 실험마다 질문이 나오고 Enter로 실행, q로 종료합니다. 자동 검증은 `./play-redis-02 verify`.

## 무엇을 옮겨 왔나

블로그의 조회수 기록을 작게 옮겼습니다(`Views`). 조회수 원본은 PostgreSQL에 있고 Redis에는 잃어도 되는 두 가지만 둡니다.

| 키 | 명령 | 역할 |
|---|---|---|
| `view:{글}:{세션 SHA-256}` | `SET NX EX 86400` | 이 세션의 첫 조회인지 판정 |
| `popular:DEV:{날짜}` | `ZINCRBY`, `EXPIRE 8일` | 그날 글별 조회수 |

조회 기록 순서: `SET NX` → (처음이면) PostgreSQL `view_count +1` → `ZINCRBY` → `EXPIRE`. Redis 명령이 실패하면 그 조회는 세지 않습니다.

Lettuce 6.8.1 설정은 블로그 운영과 같습니다: 명령 타임아웃 300ms(`TimeoutOptions`), 연결·핸드셰이크 1초(`RedisURI` timeout), 자동 재연결, 연결이 끊긴 동안 명령 즉시 거부(`REJECT_COMMANDS`). Redis는 실행마다 임시 디렉터리·임의 포트로 띄우고, 기본 사용자에 운영과 같은 ACL(`+@all -@dangerous -@admin`)을 겁니다. 관측(INFO·CONFIG·SCAN)은 실행마다 새 비밀번호를 쓰는 별도 사용자로만 합니다.

비교용 두 방식도 있습니다.

- `POSTGRES`: 한 트랜잭션에서 `content_view`에 `INSERT … ON CONFLICT DO NOTHING`, 처음이면 `view_count +1`
- `REDIS_SCRIPT`: `SET NX`·`ZINCRBY`·`EXPIRE`를 Lua 스크립트 한 번(`EVAL`)으로. 블로그 운영에는 없는 방식입니다

## 실험

| 실험 | 장애 | 비교 |
|---|---|---|
| 1 Redis가 죽으면 | 10ms 간격 조회 중 `kill -9`, 1초 뒤 재시작 | 연결 끊긴 동안 즉시 거부 / Lettuce 기본값(쌓아 둠) |
| 2 Redis가 느리면 | Redis 응답만 0·100·250·400ms 늦춤, 뒤에 같은 세션으로 다시 조회 | 지연별 |
| 3 Redis가 재시작하면 | 새 세션 2,000개 조회 직후 `kill -9`, 같은 디렉터리로 재시작, 같은 세션으로 다시 조회 | AOF 없음 / `everysec` / `always` |
| 4 Redis가 꽉 차면 | `maxmemory`를 사용량 + 1MB로, 새 세션 30,000개 조회 뒤 처음 5,000개로 다시 조회 | `allkeys-lru` / `volatile-ttl` / `noeviction` |
| 5 네트워크를 건너면 | Redis·PostgreSQL 앞에 같은 왕복 지연 0·1·5ms | `POSTGRES` / `REDIS` / `REDIS_SCRIPT` |

## 주의

- 지연은 루프백 TCP 프록시(`DelayProxy`)가 넣습니다. 받은 덩어리마다 "받은 시각 + 지연"에 넘기므로 덩어리가 여러 개여도 지연이 쌓이지 않습니다. macOS에서는 대기 시간이 조금씩 넘쳐 실제 왕복은 넣은 값보다 깁니다(5ms를 넣으면 약 6ms).
- `kill -9`는 프로세스 종료입니다. 운영체제나 전원이 꺼지는 경우(페이지 캐시 유실)는 실험하지 않습니다.
- `always`의 쓰기 비용은 이 Mac의 fsync 비용이며 서버 디스크에서는 다릅니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
