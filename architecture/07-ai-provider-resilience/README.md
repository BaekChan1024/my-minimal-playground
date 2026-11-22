# Architecture 07 — AI 공급자가 실패할 때: 재시도·Circuit Breaker·Bulkhead

상세 글: [블로그에서 읽기](https://blog.baekchan.com/post/ai-공급자가-실패하면-다시-보낼까-재시도circuit-breakerbulkhead를-실험하고-운영에-붙이기)

JDK21과 Git이 필요합니다. 임시 PostgreSQL 18.4와 가짜 AI 공급자(이 프로세스 안의 로컬 HTTP 서버)를 실행합니다. 실제 AI 공급자는 호출하지 않으며 Docker나 외부 DB도 필요 없습니다. 첫 실행 시 Maven Central에서 의존성(PostgreSQL 바이너리, Resilience4j 2.4.0 포함)을 받습니다. macOS ARM64에서 검증했으며 다른 OS는 미검증입니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach resilience-01-v1
./play-resilience-01
```

Mac에서는 `Resilience-01.command`를 더블클릭해도 됩니다. 실험마다 질문이 나오고 Enter로 실행, q로 종료합니다. 자동 검증은 `./play-resilience-01 verify`(약 1분).

블로그 Content 서비스의 AI 작업 대기열(`ai_job`)과 워커를 줄여 옮겼습니다. 작업은 PostgreSQL 행이고, 워커는 `FOR UPDATE SKIP LOCKED`로 하나씩 집어 공급자를 부릅니다. 재시도할 실패면 작업을 대기열로 되돌리고 다음 시도 시각을 행에 적습니다(스레드에서 쉬지 않음). 시간은 실습에 맞게 줄였습니다(운영의 30초 → 0.2초 등).

## 실패 분류 (`ProviderClient`)

| 코드 | 언제 | 공급자가 처리했을 수 있나 |
|---|---|---|
| `PROVIDER_UNREACHABLE` | 연결 거부·연결 시간 초과 | 아니오 |
| `PROVIDER_UNAVAILABLE` | 502·504를 뺀 5xx | 아니오 |
| `PROVIDER_RATE_LIMITED` | 429(`Retry-After` 초) | 아니오 |
| `PROVIDER_TIMEOUT` | 요청을 보낸 뒤 응답 시간 초과, 504 | 예 |
| `PROVIDER_CONNECTION_LOST` | 요청을 보낸 뒤 응답 없이 끊김, 502 | 예 |

## 워커 스위치(`Worker.Options`)

| 스위치 | 하는 일 |
|---|---|
| `threads` | 워커 스레드 수 = 동시 호출 수 |
| `retry` | `NONE` / `SAFE_ONLY`(처리하지 않았다고 확신할 수 있는 실패만) / `ALL`(시간 초과·끊김까지) |
| `maxRetries`, `initialBackoffMs`, `maxBackoffMs` | 3번, 0.2초부터 두 배, 최대 2초. 절반은 무작위(equal jitter). `Retry-After`가 더 길면 그것 |
| `breaker` | Resilience4j Circuit Breaker. 작업을 집기 전에 허가를 묻는다. `contentBreaker()`: 최근 6번 중 3번 이상일 때 실패 50% 이상이면 열고 1초 뒤 시험 호출 1번 |
| `systemSlots` | 스레드 중 시스템 작업(자동 요약)에 쓸 수 있는 수(Resilience4j Bulkhead, 대기 0) |
| `idempotencyKey` | 요청에 `Idempotency-Key`를 싣는다. 가짜 공급자는 같은 키를 다시 처리하지 않는다 |

## 실험

| 실험 | 장애 | 비교 |
|---|---|---|
| 1 공급자 장애 | 작업 30개, 공급자가 2.5초 동안 503 | 재시도 없음 / 재시도만 / 재시도 + Circuit Breaker |
| 2 응답 유실 | 작업 10개, 첫 호출을 처리·과금한 뒤 응답을 잃음(5개 시간 초과, 5개 끊김) | 안전한 실패만 재시도 / 모두 재시도 / 모두 재시도 + 멱등키 |
| 3 느린 성공 | 작업 6개, 실패 없이 1.2초씩 | 느린 호출 규칙 Resilience4j 기본값(60초를 1초로 줄임) / 느린 호출 규칙 끔 |
| 4 자동 요약 몰림 | 자동 요약 20개 뒤 0.1초 늦게 저자 요청 1개, 호출 0.3초 | 스레드 1개 / 2개 / 2개 중 자동 요약 1개까지 |

## 주의

- 공급자는 흉내이고, 시간 초과·끊김·멱등키 동작은 이 실습에서 정한 규칙입니다. 실제 AI 공급자가 멱등키를 지원한다는 뜻이 아닙니다.
- 시간 값은 실습용으로 줄였습니다. 실험 3은 Resilience4j 기본값의 느린 호출 기준(60초)만 1초로 줄였고 비율(100%)은 기본값 그대로입니다.
- Circuit Breaker와 Bulkhead는 워커 한 개(프로세스 하나) 안에서만 셉니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
