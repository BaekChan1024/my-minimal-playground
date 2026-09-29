# 검증 기록

2026-09-29, macOS ARM64/JDK21. Boot4.0.1 BOM과 gradle.lockfile 사용.

`JAVA_HOME=$(/usr/libexec/java_home -v 21) ./gradlew :spring:04-read-consistency:verifyLab --write-locks --console=plain` exit0.

```text
Spring=7.0.2 Hibernate=7.2.0.Final PostgreSQL=18.4 JDBC=42.7.10
OBS jdbc-rc first=original second=updated separateWriter=true
PASS jdbc-rc afterReader=updated
OBS jdbc-rr first=original second=original separateWriter=true
PASS jdbc-rr afterReader=updated
OBS jpa-rc secondFind=original sameObject=true secondFindSql=0 jdbc=updated scalar=updated refresh=updated clearFind=updated hibernateSelects=4
PASS jpa-rc afterReader=updated
OBS jpa-rr secondFind=original sameObject=true secondFindSql=0 jdbc=original scalar=original refresh=original clearFind=original hibernateSelects=4
PASS jpa-rr afterReader=updated
OBS readonly-count-list-rc readOnly=on totalBefore=1 ids=[1, 2] totalAfter=2
PASS readonly-count-list-rc afterReaderCount=2
OBS readonly-count-list-rr readOnly=on totalBefore=1 ids=[1] totalAfter=1
PASS readonly-count-list-rr afterReaderCount=2
ALL 6 SCENARIOS PASSED. 비교 해설: spring/04-read-consistency/ANSWERS.md
```

각 reader의 SHOW transaction_isolation/read_only를 assert합니다. 별도 writer 연결의 backend PID를 비교하고 명시적 commit 완료 뒤 두 번째 읽기를 실행합니다. JPA 두 번째 find의 객체 동일성과 추가 SELECT0을 검사합니다. StatementInspector는 Hibernate SELECT만 세며 JDBC 관측 쿼리는 포함하지 않습니다. JPA 실험마다 최초 find/scalar/refresh/clear 후 find 총4개를 확인합니다. 최종 reader 바깥 DB 값과 행 수로 writer의 커밋도 검증합니다.

추가 실행: 저장소 밖 /private/tmp에서 Spring-04.command에 Enter 전달, 여섯 PASS와 정상 종료 확인.

미검증: 운영 설정/격리 수준, 프로덕션 HTTP, 2차 캐시·쿼리 캐시, pending changes가 있는 refresh/clear, 모든 JPA query 유형, readOnly 쓰기 거부 오류, Serializable·쓰기 스큐·교착상태·성능·장기 트랜잭션의 비용, 다른 DB/OS. Kafka·Outbox·Inbox는 실행하지 않습니다.
