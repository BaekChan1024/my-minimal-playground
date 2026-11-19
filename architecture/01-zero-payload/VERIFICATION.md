# 검증 — 2026-11-19

macOS ARM64 / OpenJDK 21.0.6 / Gradle 9.2.1 / Kafka 4.1.1 (KRaft, 브로커1·컨트롤러1, metadata 4.1-IV1) / Jackson 2.19.0 / JDK 내장 HttpServer·HttpClient(loopback).
토픽은 실험·형식마다 새로 만들고 파티션 1개, 복제 계수 1, key는 글 ID입니다. Producer는 `acks=all`, `linger.ms=0`, 나머지는 기본값(`max.request.size` 1,048,576 포함).
소비자는 자동 커밋을 끄고 수동 할당으로 그룹의 커밋 위치부터 읽습니다. 처리에 성공한 레코드까지만 `commitSync`하고, 재시도할 레코드로 seek한 뒤 100ms 쉽니다.

## 자동 검증 출력

```text
PASS order sent=[v2,v1] fatNaive=[v2:제목 B, v1:제목 A] fatGuarded=[v2:제목 B, skip v1] zero=[v2:제목 B, v2:제목 B] zeroFetched=[2, 2] apiCalls=2
PASS size normalBody=6000B fatEvent=6152B zeroEvent=88B largeBody=1200000B largeFatEvent=1200149B fat=RecordTooLargeException zero=sent(88B) fatTopicRecords=0
PASS delete fatSteps=[v1:공개 글, absent, absent] zeroSteps=[absent, absent, absent] fatRecordsWithText=2 zeroRecordsWithText=0 apiCalls=3
PASS outage fat=applied(committed=1) zeroDuring=handled:0,committed:0,retries:4,http503:4,last=http 503 zeroAfter=applied(committed=1)
PASS cost events=200 contents=20 fatCalls=0 zeroEachCalls=200 zeroCoalescedCalls=20 coalescedBatches=1 fatTopicBytes=632040 zeroTopicBytes=18000
INFO cost singleRunMs fat=523.6 zeroEach=509.6 zeroCoalesced=514.2 batches fat=1 each=1 (not a benchmark)
PASS replica zero=[v1:제목 v1](committed=1, noMoreEvents) zeroVersion=retries:3,last=stale read v1 < event v2 then [v2:제목 v2](committed=1)
PASS experiments=6 realBroker=true loopbackHttp=true
```

`verify` 4회, 안내 실행(모든 Enter) 1회 모두 종료 코드 0. 실행마다 같았던 값: 판정 대상 전부, 바이트 수, `retries:4`(장애). 달랐던 값: 복제 지연 재시도 2~3회, INFO의 시간 값.
안내 실행 중 2번 질문에서 q를 입력하면 자원을 닫고 종료 코드 0으로 끝나는 것을 확인했습니다. 잘못된 인자는 사용법과 종료 코드 2.

## 판정 기준

| 실험 | 판정 |
|---|---|
| 1 순서 | 그대로 덮어쓰기 = 제목 A, version 비교 = 제목 B(건너뜀 1), zero = 제목 B, 조회 결과 version [2, 2], API 호출 2 |
| 2 크기 | 1.2MB 본문 fat event가 `RecordTooLargeException`, fat 토픽 0건, zero event 오프셋 0으로 전송 |
| 3 삭제 | fat 조회 모델에 삭제된 글이 1단계 노출, zero 0단계. 토픽에서 문장을 포함한 레코드 fat 2건, zero 0건. 최종 둘 다 없음 |
| 4 장애 | fat 처리·커밋 1. zero는 1.5초 동안 처리 0, 커밋 0, 재시도 2회 이상. 복구 후 재시도 없이 커밋 1 |
| 5 비용 | fat 호출 0, zero 이벤트당 호출 200, 병합 호출 20~200. 세 모델 모두 글 20개가 version 10과 마지막 제목 |
| 6 복제 지연 | ID만: v1 저장·커밋 1, 이후 처리할 이벤트 0. ID+version: 지연 동안 커밋 0·재시도 2회 이상, 동기화 후 v2·커밋 1 |

## 바꿔 보기 확인

병합 소비자(`cost-merged`)의 `max.poll.records`를 10으로 바꾼 실행: `zeroCoalescedCalls=200 coalescedBatches=20`, 판정 통과. 확인 후 원래 값(500)으로 되돌렸습니다.

## 검증하지 않은 범위

- 원본 저장소는 메모리 맵입니다. 실제 DB 트랜잭션·Outbox·복제는 없습니다. 복제 지연은 조회 결과가 늦는 조건만 흉내 냈습니다.
- 순서 역전은 전송 순서를 직접 뒤집어 만들었습니다. 릴레이 경쟁, 파티션 변경, 재시도에 의한 실제 역전은 재현하지 않았습니다.
- 시간 값은 소비자 시작·첫 poll이 대부분이고 loopback 메모리 조회라 네트워크·DB 비용을 보여 주지 않습니다. 부하·동시 소비자·캐시·원본 서비스 과부하는 측정하지 않았습니다.
- 브로커 `message.max.bytes`, 토픽 `max.message.bytes`, 압축을 바꾼 큰 메시지 전송은 다루지 않았습니다.
- 재시도는 무한 반복입니다. DLT, 백오프 상한, 서킷 브레이커, 같은 파티션 뒤 레코드의 대기(head-of-line)는 판정하지 않았습니다.
- 생성 직후 조회가 404를 돌려주는 경우(복제 전 생성)와 삭제로 인한 404를 version만으로 구분하는 방법은 다루지 않았습니다.
