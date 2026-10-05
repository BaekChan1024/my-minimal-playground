# Database 08 — COUNT(*)는 왜 오래 걸릴까? — 집계와 읽는 양

[블로그에서 상세 해설 읽기](https://blog.baekchan.com/post/count는-왜-오래-걸릴까-집계와-읽는-양)

JDK 21과 Git으로 실행하는 독립 PostgreSQL 실습입니다. Docker나 별도 DB 설치는 필요 없습니다.
첫 실행에는 Gradle 의존성과 PostgreSQL 바이너리를 내려받을 네트워크가 필요합니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach database-08-v1
./play-database-08
```

기존 저장소는 변경을 보존하고 `git fetch origin tag database-08-v1`로 가져오세요.
Mac에서는 `Database-08.command`, 자동 검증은 `./play-database-08 verify`입니다.
안내 모드의 예상 질문에 답해 본 뒤 Enter로 실행하세요. q를 입력하면 시작하지 않습니다.

[문제](EXERCISES.md) → 직접 실행 → [해설](ANSWERS.md) 순서로 비교하세요.
SQL은 src/main/java 아래 실행 코드에 있으며 조건 변경은 기존 작업을 보존한 별도 복사본에서 진행하세요.
고정 조건을 바꾸면 검증 조건도 달라질 수 있습니다.

## 검증 환경과 범위

2026-10-05 macOS ARM64/JDK21.0.6/PostgreSQL18.4/JDBC42.7.10에서 자동 검증과 저장소 밖 Mac 안내 모드 모두 6개 체크포인트를 통과했습니다.
다른 OS에서의 실행 결과는 미확인입니다. 의존성은 Gradle lockfile에 고정합니다.
loopback의 임의 포트에 실제 임시 PostgreSQL을 만들고 정상 종료 시 정리합니다. 공유 서버 접속 정보는 필요 없습니다.

공유 버퍼32MB·work_mem64MB·병렬/JIT off. Index Only Scan 비교는 all-visible을 준비하고 실제 Heap Fetches0을 확인합니다. 마지막 두 연결의 삽입은 버퍼 비교 이후입니다. 별도 캐시·업무 카운터 구현이나 운영 성능 배수는 검증하지 않습니다.

[관측 기록](VERIFICATION.md)은 기준 실행의 결과이며 사용자 학습 완료나 운영 성능을 보장하지 않습니다.
