# Database 04 — 복합 인덱스의 컬럼 순서

[전체 블로그 글](https://blog.baekchan.com/post/복합-인덱스는-왜-컬럼-순서가-중요할까) · 고정 태그 `database-04-v1`

JDK21과 Git이 필요합니다. 처음에는 Gradle·PostgreSQL 바이너리 다운로드 네트워크가 필요합니다.
Docker나 별도 DB 설치 없이 loopback 임시 PostgreSQL에서 실행하고 종료 시 정리합니다.

```bash
./play-database-04
./play-database-04 verify
```

Mac은 `Database-04.command`를 실행해도 됩니다. 새 복사본에서 고정 태그로 이동하려면
`git switch --detach database-04-v1`을 사용하세요. 기존 복사본에서는 변경을 보존한 뒤
`git fetch origin tag database-04-v1`로 태그를 가져오세요.

## 실행 전 예상

- 사용자 한 명의 범위 조회와 전체 최신20개에 같은 컬럼 순서가 유리할까요?
- WHERE 조건을 적는 순서와 인덱스 키 순서는 같은 의미일까요?
- 선두 컬럼 조건이 없으면 PostgreSQL18도 인덱스를 전혀 못 쓸까요?

Enter로 여섯 검증 항목을 실행합니다. `PLAN_JSON`은 원본 계획, `NODE`는 각 노드의 관측,
`RESULT`는 최상위 버퍼 접근과 정렬 여부입니다. [문제](EXERCISES.md)를 먼저 읽고
[해설](ANSWERS.md)과 비교하세요.

## 격리 조건

동일한10만 행의 세 테이블에 각각 인덱스를 하나씩 생성합니다.
(tenant_id,seq), (seq,tenant_id), (tenant_id ASC,seq DESC)입니다.
실습 테이블의 일반 autovacuum을 제외하고 통계 목표1000, 병렬 조회0, JIT off를 사용합니다.
준비 시 VACUUM 후 all-visible 페이지 조건을 검사하며 최대3번의 준비 정리에도 맞지 않으면 중단합니다.
읽기 중 쓰기는 없고 인덱스 키만 반환합니다. 실제 스캔 종류는 강제하지 않습니다.

이것은 운영 설정 권고나 응답 시간 벤치마크가 아닙니다.
세부 관측과 미검증 범위는 [VERIFICATION.md](VERIFICATION.md)를 참고하세요.
