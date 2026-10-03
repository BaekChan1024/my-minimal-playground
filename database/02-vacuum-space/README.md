# Database 02 — VACUUM과 공간 재사용

[블로그 글: DELETE했는데 DB 용량은 왜 그대로일까?](https://blog.baekchan.com/post/delete했는데-db-용량은-왜-그대로일까-vacuum과-공간-재사용)

JDK 21, Git, 최초 의존성 다운로드를 위한 인터넷 연결이 필요합니다. Docker·별도 DB 설치는 필요 없습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach database-02-v1
./play-database-02
```

기존 저장소에서는 수정 사항을 보존한 뒤 `git fetch origin tag database-02-v1`로 태그를 받으세요.
Mac에서는 루트의 `Database-02.command`를 더블클릭할 수 있습니다. 다른 디렉터리에서 시작해도 저장소를 찾아갑니다.
설치된 JDK 21을 확인하며 자동 설치하거나 시스템 설정을 바꾸지 않습니다.

- 예상 질문에 답한 뒤 Enter로 실행합니다. `q`는 DB를 시작하지 않고 종료합니다.
- 자동 검증: `./play-database-02 verify`
- Gradle: `./gradlew :database:02-vacuum-space:verifyLab --console=plain`
- Windows: `gradlew.bat :database:02-vacuum-space:verifyLab --console=plain` (실행 미검증)

임시 PostgreSQL을 loopback 임의 포트에서 시작하고 정상 종료 시 정리합니다. 외부 DB 설정을 읽지 않습니다.
실습 전용 테이블에만 `autovacuum_enabled=false`를 설정하고, 임시 DB에 `pgstattuple` 확장을 만듭니다. 운영 권장 설정이 아닙니다.
최초 실행에서 PostgreSQL 바이너리와 라이브러리를 내려받습니다. 확인한 환경은 macOS ARM64 / Java 21.0.6 / PostgreSQL 18.4 / pgjdbc 42.7.10입니다.
Spring Boot BOM은 의존성 버전 관리용이며 웹 서버나 Spring 트랜잭션 매니저를 사용하지 않습니다.

## 실습 순서

[예상·실습 문제](EXERCISES.md)를 먼저 읽고 실행 후 [해설](ANSWERS.md)과 비교하세요.

1. 20,000행 중 18,000행 삭제와 heap 크기
2. 일반 VACUUM 뒤 내부 빈 공간
3. 18,000행 재삽입과 heap 재사용
4. 3,800행을 남긴 상태의 VACUUM FULL 재작성
5. 오래 유지한 REPEATABLE READ 스냅샷과 정리 지연
6. 트랜잭션 블록 안에서의 VACUUM 거절

실측 바이트와 검증 범위는 [VERIFICATION.md](VERIFICATION.md)에 있습니다. `count(*)` 등의 관측 접근도 페이지 정리에 영향을 줄 수 있으므로 DELETE 수와 측정 dead tuple 수가 일치한다고 가정하지 않습니다.
일반 VACUUM은 `TRUNCATE FALSE`로 실행하여 파일 끝의 빈 페이지 절단을 제외했습니다. 이것은 테이블 전체를 비우는 `TRUNCATE` 명령과 다릅니다.

실습은 테이블 파일 크기와 내부 공간을 측정합니다. 서버 디스크 여유량, 운영 처리량, WAL, autovacuum 주기, 최대 임시 디스크 사용량은 검증하지 않습니다.
