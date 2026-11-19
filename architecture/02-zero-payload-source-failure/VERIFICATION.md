# 검증 — 2026-11-19

macOS ARM64 / OpenJDK 21.0.6 / Gradle 9.2.1 / Kafka 4.1.1 (KRaft, 브로커1·컨트롤러1, metadata 4.1-IV1) / Jackson 2.19.0 / JDK 내장 HttpServer(작업 스레드 16)·HttpClient(loopback).
Producer `acks=all`, `linger.ms=0`. 소비자는 자동 커밋 끔, `max.poll.records=500`(5번 제외), classic 그룹 프로토콜, RangeAssignor.
1~3번 소비자는 수동 할당이고 `fetch.max.wait.ms=100`, 재시도 간격 100ms, DLT 기준 같은 레코드 3번 실패, `/health` 장애 시 200ms 멈춤(그동안 pause한 채 poll 유지).

## 자동 검증 출력 (마지막 실행)

```text
PASS headOfLine during(2s): p0 committed=0 applied=0/10, p1 committed=10 applied=10/10, retries=16 | after: p0 committed=10 p1 committed=10 applied=20
PASS dlt first: zero dead=1 retries=2 others=applied | beforeReplay: zero=제목 v2,fatNaive=제목 v2,fatGuarded=제목 v2 | replay v1: zero=제목 v2(apiCalls=1) fatNaive=제목 v1 fatGuarded=제목 v2(skipped)
PASS outage retryThenDlt: dead=13 processed=17 retries=27 | healthGated: dead=0 processed=30 pauses=15 retries=0
PASS slow latency=20ms events=200 contents=50 final=v4 | 1-each calls=200 batches=1 handlerMs=5385 | 2-dedupe calls=50 batches=1 handlerMs=1280 | 3-bulk calls=1 batches=1 handlerMs=34 | 4-parallel8 calls=200 batches=1 handlerMs=668
SEEN pollInterval maxPollIntervalMs=2000 latency=30ms | maxPollRecords=100: fetches=300 commitsOk=0 commitsFailed=3(CommitFailedException) committed=0 in 10999ms | maxPollRecords=20: fetches=100 commitsOk=5 commitsFailed=0 committed=100
PASS pollInterval batch100=neverCommitted batch20=committedOnce
PASS experiments=5 realBroker=true loopbackHttp=true
```

`fetch.max.wait.ms`·장애 3초를 적용한 뒤 `verify` 4회, 안내 실행(모든 Enter) 1회 모두 종료 코드 0. 실행마다 같았던 값: 판정 대상 전부, DLT 13건/처리 17건, health 멈춤 15회, 조회 횟수, 5번의 조회 300·커밋 실패 3. 달랐던 값: 1번 재시도 16~17, 3번 재시도 27~28, 처리 시간(이벤트마다 4,823~5,385ms, 중복 제거 1,174~1,280ms, 여러 건 26~34ms, 동시 582~668ms), 5번 경과 시간.
안내 실행 2번 질문에서 q를 입력하면 자원을 닫고 종료 코드 0. 잘못된 인자는 사용법과 종료 코드 2.

### 설정을 바꾸기 전 첫 실행 (기록용)

`fetch.max.wait.ms` 기본값(500)·장애 1.5초에서는 `headOfLine retries=4`(2초 동안), `outage retryThenDlt: dead=1 processed=29 retries=3`, `healthGated: dead=0 pauses=3`이었습니다. 재시도마다 seek 직후에도 진행 중인 빈 fetch가 끝날 때까지 기다려 간격이 약 0.5초로 늘어난 것으로 판단해 설정을 바꿨습니다. 1번 판정 기준(재시도 5회 이상)은 이 첫 실행에서 실패했습니다.

## 판정 기준

| 실험 | 판정 |
|---|---|
| 1 파티션 막힘 | 2초 동안 p0 커밋 0·반영 0, p1 커밋 10·반영 10, 재시도 5회 이상. 조회 복구 후 20건 모두 반영 |
| 2 DLT 재처리 | zero가 3번째 실패에서 DLT 1건(재시도 2), 다른 글은 반영. DLT에 v1. v2 반영 뒤 v1 재처리: zero = v2, fat 덮어쓰기 = v1, fat version 비교 = 건너뜀(v2) |
| 3 원본 전체 장애 | DLT 정책: DLT ≥ 1건, DLT 토픽 건수 일치, 처리 + DLT = 30. health 정책: DLT 0, 처리 30, 멈춤 2회 이상 |
| 4 느린 원본 | 네 방식 모두 글 50개가 v4. 이벤트마다: 조회 200, 처리 시간 ≥ 4,000ms. 중복 제거: 50~200. 여러 건: 배치당 1회. 동시: 200 |
| 5 max.poll.interval.ms | 100건 배치: 커밋 실패 ≥ 1, 최종 커밋 0, 조회 > 100. 20건 배치: 커밋 실패 0, 커밋 100, 조회 100 |

## 바꿔 보기 확인

`MAX_POLL_INTERVAL_MS = "5000"`: `SEEN ... maxPollRecords=100: fetches=100 commitsOk=1 commitsFailed=0(-) committed=100 in 3398ms`, 이어서 `FAIL 100-record batches never commit and re-fetch`, 종료 코드 1. 확인 후 되돌렸습니다.

## 검증하지 않은 범위

- 지연은 서버 sleep이고 원본 저장소는 메모리입니다. 실제 DB·네트워크, 원본 과부하와 그로 인한 연쇄 지연은 측정하지 않았습니다. 처리 시간은 단일 실행 값이며 벤치마크가 아닙니다.
- `/health`가 장애를 정확히 반영하는 조건만 다뤘습니다. 일부 인스턴스만 실패하거나 health는 정상인데 조회만 느린 경우는 없습니다.
- 서킷 브레이커 라이브러리, 지수 백오프·지터, 재시도 토픽 단계, DLT 재처리 자동화, 원본 쪽 rate limit은 구현하지 않았습니다.
- 5번은 classic 그룹 프로토콜만 확인했습니다. consumer 그룹 프로토콜(KIP-848)과 cooperative 할당은 다루지 않았습니다.
- 동시 조회는 같은 글을 동시에 조회해도 결과가 같은 조건(모든 변경이 끝난 뒤 소비)에서만 확인했습니다.
