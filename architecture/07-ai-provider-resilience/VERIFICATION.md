# 검증 — 2026-11-22

macOS ARM64 / OpenJDK 21.0.6 / Gradle 9.2.1 / PostgreSQL 18.4 (zonky embedded-postgres 2.2.2) / HikariCP 7.0.2 / Resilience4j 2.4.0 (circuitbreaker, bulkhead) / JDK `HttpServer`·`HttpClient`.
변형마다 새 database와 새 가짜 공급자를 만듭니다. 워커는 스레드마다 `FOR UPDATE SKIP LOCKED`로 작업을 하나 집고, 할 일이 없으면 20ms 쉽니다. 클라이언트 응답 시간 초과는 실험 1·3·4에서 2~3초, 실험 2에서 0.5초이고, 가짜 공급자의 "처리 후 붙잡기"는 1.5초입니다.

## 자동 검증 출력

```text
PASS outage variant=noRetry outcomes={FAILED:PROVIDER_UNAVAILABLE=30} callsDuringOutage=30 totalCalls=30 retriesUsed=0 drainedMs=202
PASS outage variant=retry outcomes={FAILED:PROVIDER_UNAVAILABLE=30} callsDuringOutage=120 totalCalls=120 retriesUsed=90 drainedMs=1416
PASS outage variant=retry+breaker outcomes={SUCCEEDED=30} callsDuringOutage=6 totalCalls=36 retriesUsed=6 drainedMs=4023 breakerTransitions=7
PASS lostResponse variant=safeOnly outcomes={FAILED:PROVIDER_CONNECTION_LOST=5, FAILED:PROVIDER_TIMEOUT=5} providerCalls=10 charged=10 jobsChargedTwice=0
PASS lostResponse variant=retryAll outcomes={SUCCEEDED=10} providerCalls=20 charged=20 jobsChargedTwice=10
PASS lostResponse variant=retryAll+idempotencyKey outcomes={SUCCEEDED=10} providerCalls=20 charged=10 jobsChargedTwice=0
PASS slowSuccess variant=slowCallDefaults outcomes={SUCCEEDED=6} failedCalls=0 opened=4 state=OPEN drainedMs=10277
PASS slowSuccess variant=slowCallOff outcomes={SUCCEEDED=6} failedCalls=0 opened=0 state=CLOSED drainedMs=7257
PASS burst variant=oneThread authorWaitMs=6062 systemJobsDoneAfterMs=6170 maxConcurrentCalls=1
PASS burst variant=twoThreads authorWaitMs=2976 systemJobsDoneAfterMs=3079 maxConcurrentCalls=2
PASS burst variant=twoThreads+systemSlot1 authorWaitMs=12 systemJobsDoneAfterMs=6169 maxConcurrentCalls=2
PASS experiments=4 realPostgres=true
```

`verify` 3회, 새로 복제한 디렉터리에서 `./play-resilience-01 verify` 2회, 안내 실행(모든 Enter) 1회, 모두 종료 코드 0.
실행마다 같았던 값: 판정 대상 전부, 결과 분포, 호출 수(30·120·6/36·10·20·20), 재시도 수(0·90·6), 비용(10·20·10), 차단기 열림 횟수(4·0)와 전이 수(7).
달랐던 값: 시간(ms). 여섯 번(verify 3, 새 복제본 2, 안내 1)의 범위는 다음과 같습니다.

| 값 | 범위 (ms) |
|---|---|
| 실험 1 drainedMs: 재시도 없음 / 재시도만 / 재시도 + 차단기 | 165~202 / 1,254~1,416 / 3,939~4,023 |
| 실험 3 drainedMs: 기본값 / 끔 | 10,277~10,314 / 7,240~7,271 |
| 실험 4 authorWaitMs: 스레드 1 / 2 / 2 + 자동 요약 1 | 6,017~6,062 / 2,950~2,979 / 8~17 |
| 실험 4 systemJobsDoneAfterMs: 스레드 1 / 2 / 2 + 자동 요약 1 | 6,123~6,170 / 3,055~3,083 / 6,091~6,169 |
두 번째 질문에서 q를 입력하면 임시 DB를 정리하고 종료 코드 0으로 끝나는 것을 확인했습니다. 잘못된 인자는 사용법과 종료 코드 2.

## 판정 기준

| 실험 | 판정 |
|---|---|
| 1 | 재시도 없음: 30개 `PROVIDER_UNAVAILABLE` 실패·호출 30. 재시도만: 30개 실패·장애 중 호출 120. 재시도 + 차단기: 30개 성공·장애 중 호출 12 이하 |
| 2 | 안전한 실패만: 시간 초과 5·끊김 5 실패, 비용 10·두 번 과금 0. 모두 재시도: 10개 성공, 비용 20·두 번 과금 10. 모두 재시도 + 멱등키: 10개 성공, 비용 10·두 번 과금 0 |
| 3 | Resilience4j 기본 느린 호출 비율 100%·기준 60초 확인. 기본값(60초→1초): 6개 성공, 실패 0인데 `CLOSED → OPEN` 전이 있음. 끔: 6개 성공, 전이 없음 |
| 4 | 21개 모두 성공. 자동 요약 1개까지: 저자 대기 500ms 미만. 스레드 1개 대기 > 스레드 2개 대기 > 1000ms |

## 바꿔 보기 확인

실험 1의 장애를 0.6초로 바꾼 실행: 재시도 없음 통과 후 `FAIL retry all failed {SUCCEEDED=30}`로 멈춤(종료 코드 1). 확인 후 되돌렸습니다.

## 검증하지 않은 범위

- 실제 AI 공급자는 호출하지 않았습니다. 시간 초과·끊김·멱등키는 가짜 공급자의 규칙입니다.
- 워커는 프로세스 하나입니다. 여러 인스턴스가 각자 차단기·Bulkhead를 셀 때는 실험하지 않았습니다.
- 재시도 간격의 jitter가 몰림을 얼마나 줄이는지는 따로 재지 않았습니다.
- 차단기가 열린 동안 오래 기다린 작업의 만료(Content의 60분 `EXPIRED`)는 이 실습에 없습니다.
