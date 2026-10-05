# Database 03 — 실행 계획과 통계

[블로그에서 전체 글 읽기](https://blog.baekchan.com/post/인덱스를-만들었는데-왜-안-쓸까-실행-계획과-통계) · 고정 태그 `database-03-v1`

JDK 21과 Git이 필요합니다. 첫 실행은 의존성을 다운로드하므로 네트워크가 필요합니다.
Docker나 별도 PostgreSQL 설치 없이 loopback의 임시 DB에서 실행하고 종료 시 정리합니다.

```bash
./play-database-03
./play-database-03 verify
```

Mac에서는 `Database-03.command`로 안내 모드를 실행할 수도 있습니다.
기존 변경을 보존한 뒤 태그를 가져오세요: `git fetch origin tag database-03-v1`.
새 복사본에서는 `git switch --detach database-03-v1` 후 실행합니다.

## 실행 전 예상

- 10만 행 중 100행과 9만 행을 읽을 때 같은 인덱스를 쓸까요?
- 분포가 바뀌었지만 통계는 같다면 예상 행 수는 어떻게 될까요?
- 통계가 정확해지면 버퍼 접근도 반드시 줄어들까요?

Enter로 진행하면 원본 EXPLAIN JSON, 요약 RESULT, 통계 STATS와 여섯 PASS가 출력됩니다.
[EXERCISES.md](EXERCISES.md)를 먼저 읽고 실행 후 [ANSWERS.md](ANSWERS.md)와 비교하세요.

## 격리 조건과 범위

PostgreSQL 18.4, Java 21, pgjdbc 42.7.10과 의존성을 lockfile에 고정했습니다.
서버 전체 autovacuum은 끄지 않으며 두 실습 테이블만 자동 정리·분석 대상에서 제외합니다.
통계 목표 1000, 병렬 조회 0, JIT off는 해석을 위한 실습 조건입니다.
스캔 설정 변경은 같은 연결에서 비교 후 복원합니다. 운영 설정 예시가 아닙니다.

통계 갱신 전후에는 동일한 데이터를 조회합니다. 앞서 VACUUM을 완료하고 ANALYZE만 비교합니다.
검증은 행 수·실행 경로·좁은 조회의 버퍼 작업량을 확인합니다. 시간 임계값, 보편적인 선택도 임계값,
ANALYZE의 성능 개선을 보장하지 않습니다. [검증 결과와 한계](VERIFICATION.md).
