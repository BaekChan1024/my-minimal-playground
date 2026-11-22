# 검증 — 2026-11-22

macOS ARM64 / OpenJDK 21.0.6 / Gradle 9.2.1 / PostgreSQL 18.4 (zonky embedded-postgres 2.2.2) / HikariCP 7.0.2. Kafka는 쓰지 않습니다.
실험마다 새 Usage·Content database를 만듭니다. Saga의 각 단계는 둘 중 한 database에서 자기 트랜잭션(기본 격리 수준 READ COMMITTED)으로 커밋합니다.
동시 실행: 스레드 32개 풀에서 Saga 하나를 스레드 하나가 끝까지 실행, 단계 사이 0~2ms 휴식. 실패할 Saga는 고정 시드로 정하지만(실험 1 60건, 실험 2 61/200건, 실험 4 38/150건) 스레드 실행 순서는 고정되지 않습니다.
예약은 요약 생성 전 실패면 돌려주고, 생성 뒤 실패면 사용량으로 확정합니다.

## 자동 검증 출력

```text
PASS dirtyRead variant=naive schedule=[A until MODERATE, reader, A rest] readerSaw=A A=COMPENSATED steps=[RESERVE, READ, GENERATE, ATTACH, MODERATE!MODERATION_REJECTED] | load: sagas=200 flagged=60 listReads=222 retractedSeen=34
PASS dirtyRead variant=reorder schedule=[A until MODERATE, reader, A rest] readerSaw=null A=COMPENSATED steps=[RESERVE, READ, GENERATE, MODERATE!MODERATION_REJECTED] | load: sagas=200 flagged=60 listReads=486 retractedSeen=0
PASS dirtyRead variant=hidden schedule=[A until MODERATE, reader, A rest] readerSaw=null A=COMPENSATED steps=[RESERVE, READ, GENERATE, ATTACH, MODERATE!MODERATION_REJECTED] | load: sagas=200 flagged=60 listReads=195 retractedSeen=0
PASS lostUpdate variant=naive order1=[A=COMPENSATED B=COMPLETED final=null] order2=[A=COMPENSATED B=COMPENSATED(MODERATION_REJECTED) final=A] | load: posts=100 sagas=200 anomalies=10 busy=0 completed=139
PASS lostUpdate variant=casUndo order1=[A=COMPENSATED B=COMPLETED final=B] order2=[A=COMPENSATED B=COMPENSATED(MODERATION_REJECTED) final=A] | load: posts=100 sagas=200 anomalies=3 busy=0 completed=139
PASS lostUpdate variant=postLock order1=[A=COMPENSATED B=COMPENSATED(BUSY) final=null] order2=[A=COMPENSATED B=COMPENSATED(BUSY) final=null] | load: posts=100 sagas=200 anomalies=0 busy=99 completed=72
PASS staleRead variant=naive schedule=[A until ATTACH, author edit, A rest] editAccepted=true A=COMPLETED post.version=2 summary.version=1 staleSummary=true
PASS staleRead variant=reread schedule=[A until ATTACH, author edit, A rest] editAccepted=true A=COMPENSATED(STALE) post.version=2 summary.version=null staleSummary=false
PASS staleRead variant=postLock schedule=[A until ATTACH, author edit, A rest] editAccepted=false A=COMPLETED post.version=1 summary.version=1 staleSummary=false
PASS quota variant=reserve aFails: A=COMPENSATED B=REJECTED(QUOTA) used=0 | bothOk: A=COMPLETED B=REJECTED(QUOTA) used=600 | load: authors=50 sagas=150 falseRejections=24 overrunAuthors=0 completed=38
PASS quota variant=optimistic aFails: A=COMPENSATED B=COMPLETED used=600 | bothOk: A=COMPLETED B=COMPLETED used=1200 | load: authors=50 sagas=150 falseRejections=0 overrunAuthors=45 completed=112
PASS experiments=4 realPostgres=true
```

`verify` 3회, 이 실습 커밋을 새로 복제한 디렉터리에서 `./play-saga-04 verify` 2회, 안내 실행(모든 Enter) 1회, 모두 종료 코드 0.
정해 둔 순서의 결과(각 줄의 `| load:` 앞부분)는 `verify` 다섯 번 모두 글자 하나까지 같았습니다.
동시 실행 수치는 실행마다 달랐습니다. 여섯 번 실행의 범위:

| 실험 | 대응책 | 수치 범위 |
|---|---|---|
| 1 더티 리드 | naive | 검수 탈락 60건 중 독자가 본 것 34~59 |
| 1 더티 리드 | reorder, hidden | 0 (판정) |
| 2 갱신 손실 | naive | 이상 글 10~19 / 100, 완료 139 |
| 2 갱신 손실 | casUndo | 이상 글 1~4 / 100, 완료 139 |
| 2 갱신 손실 | postLock | 이상 글 0 (판정), BUSY 거절 95~100, 완료 68~77 |
| 4 한도 | reserve | 틀린 거절 18~24, 한도 초과 저자 0 (판정), 완료 38~41 |
| 4 한도 | optimistic | 틀린 거절 0(정의상 생길 수 없음), 한도 초과 저자 45 / 50, 완료 112 |

두 번 고쳤고, 위 범위는 두 번째 수정 뒤의 여섯 번 실행입니다. 첫째, 실험 1의 독자가 글을 하나씩 읽어 읽기 횟수가 적었기 때문에(310회, naive 1건) 한 번에 전체 목록을 읽도록 바꿨습니다. 둘째, 처음에는 실패한 Saga가 언제 실패하든 예약을 돌려줬습니다. 2편의 규칙(토큰을 쓴 뒤의 실패는 사용량으로 확정)과 어긋나므로, 생성 뒤 실패는 확정하도록 고치고 실험 4의 실패를 검수 탈락에서 생성 실패로 바꿨습니다. 정해 둔 순서의 출력은 수정 전후가 같았습니다.
안내 실행 중 2번 질문에서 q를 입력하면 자원을 닫고 종료 코드 0으로 끝나는 것을 확인했습니다. 잘못된 인자는 사용법과 종료 코드 2.

## 판정 기준

| 실험 | 판정 |
|---|---|
| 1 | naive: 독자가 A를 봄, A 보상. reorder·hidden: 독자가 아무것도 못 봄. 모두 끝난 뒤 보이는 요약 없음. 동시 실행 reorder·hidden 0 |
| 2 순서 1 | naive: B 완료, 최종 비어 있음. casUndo: B 완료, 최종 B. postLock: B가 BUSY로 보상, 최종 비어 있음 |
| 2 순서 2 | naive·casUndo: 최종 A(보상된 Saga). postLock: 최종 비어 있음. 동시 실행 postLock 이상 0 |
| 3 | naive: 수정 반영, A 완료, 요약 버전 1 ≠ 글 버전 2. reread: 수정 반영, A가 STALE로 보상, 요약 없음. postLock: 수정 거절, A 완료, 버전 일치 |
| 4 | reserve: A 생성 실패 시 B 거절·사용량 0, 둘 다 성공 시 B 거절·사용량 600. optimistic: A 생성 실패 시 B 완료·600, 둘 다 성공 시 둘 다 완료·1,200. 동시 실행 reserve 한도 초과 0 |

## 바꿔 보기 확인

실험 4의 한도를 1,200으로 바꾼 실행: 실험 1~3 통과 후 `FAIL reserve when A fails`로 멈춤(종료 코드 1). B의 예약이 함께 들어가 완료됐기 때문입니다. 확인 후 되돌렸습니다.

## 검증하지 않은 범위

- 메시지 전달이 없습니다. 단계 호출은 한 JVM 안의 메서드 호출이고, 서비스 장애·지연은 단계 사이 휴식(0~2ms)만 흉내 냅니다.
- 작업 중 표시가 남은 채 Saga가 멈추는 경우, 표시의 만료, 거절된 Saga의 재시도는 다루지 않았습니다.
- 버전 파일(version file), 교환 가능한 갱신(commutative update) 같은 다른 대응책은 실험하지 않았습니다. 한도 카운터를 더하고 빼는 갱신은 이미 교환 가능하지만 따로 비교하지 않았습니다.
- 동시 실행 수치는 이 환경에서 여섯 번 관측한 범위입니다. 부하·지연을 바꾼 비교는 하지 않았습니다.
