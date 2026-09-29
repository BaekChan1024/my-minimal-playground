# 검증 기록

2026-09-30 / macOS ARM64 / JDK21 / Boot4.0.1 BOM / gradle.lockfile.

`JAVA_HOME=$(/usr/libexec/java_home -v 21) ./gradlew :spring:05-pagination:verifyLab --write-locks --console=plain` exit0.

```text
Spring=7.0.2 PostgreSQL=18.4 JDBC=42.7.10
PASS offset-insert first=[6, 5, 4] next=[4, 3, 2] repeated=4 cursor=b2Zmc2V0OjM
PASS offset-delete first=[6, 5, 4] next=[2, 1] skippedExisting=3
PASS keyset-insert first=[6, 5, 4] next=[3, 2, 1] freshFirst=[7, 6, 5]
PASS keyset-deleted-anchor next=[3, 2, 1] anchorExists=false
PASS tied-date first=[6, 5] dateOnly=[3, 2] composite=[4, 3]
PASS null-boundary pages=[[6, 5], [4, 3], [2, 1]] naive=[] nullTail=[1]
PASS mutable-sort-key next=[2, 1] movedBeforeBoundary=3 freshFirst=[3, 6, 5]
ALL 7 SCENARIOS PASSED. Compare EXERCISES.md and ANSWERS.md.
```

저장소 밖 /private/tmp에서 Spring-05.command에 Enter를 전달한 안내 모드도 일곱 PASS와 정상 종료를 확인했습니다.

실제 임시 PostgreSQL. 각 페이지는 별도 Spring TransactionTemplate READ COMMITTED 읽기 트랜잭션, 페이지 사이 변경은 auto-commit입니다. 매 실험 초기화. OFFSET/keyset 결과 전체 ID 배열, 삭제/누락 행의 존재, 새 첫 페이지, NULL 그룹 전체 순회 종료, cursor 날짜/ID/NULL 왕복을 assert합니다.

미검증: 운영 API·브라우저 무한 스크롤, 격리 수준 비교, 부하/실행 계획/인덱스, 서명·만료·필터 결합, 역방향 조회, decoder 오류 입력 전체, 다른 OS. EXERCISES의 조건 변경은 예상 과제이며 자동 실행 결과가 아닙니다. Kafka·Outbox·Inbox 실험은 없습니다.
