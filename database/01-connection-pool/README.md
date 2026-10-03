# Database 01 — 커넥션 풀의 대기 시간

[블로그 글: 쿼리는 빠른데 API는 왜 느릴까?](https://blog.baekchan.com/post/쿼리는-빠른데-api는-왜-느릴까-커넥션-풀의-대기-시간)

준비물은 JDK 21, Git, 첫 의존성 다운로드를 위한 인터넷 연결입니다. Docker나 별도 DB 설치는 필요 없습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach database-01-v1
./play-database-01
```

기존 저장소는 수정 사항을 보존한 뒤 `git fetch origin tag database-01-v1`로 태그를 가져오세요.
Mac에서는 루트의 `Database-01.command`를 더블클릭해도 됩니다. 다른 디렉터리에서 실행해도 저장소를 찾아갑니다.
설치된 JDK 21을 확인하며 JDK를 자동 설치하거나 시스템 설정을 변경하지 않습니다.

- 예상 질문에 답을 생각하고 Enter로 실행합니다. `q`는 DB를 시작하지 않고 종료합니다.
- 자동 검증: `./play-database-01 verify`
- Gradle 직접 실행: `./gradlew :database:01-connection-pool:verifyLab --console=plain`
- Windows 명령: `gradlew.bat :database:01-connection-pool:verifyLab --console=plain` (실행 미검증)

각 실행은 실제 PostgreSQL을 loopback 임의 포트에 띄웁니다. 정상 종료 시 임시 DB를 정리합니다.
외부 DB 설정을 읽지 않고 운영 시스템에 접속하지 않습니다. 관측 전용 연결은 Hikari 풀 밖에서 사용합니다.
검증 환경은 macOS ARM64, Java 21.0.6, PostgreSQL 18.4, pgjdbc 42.7.10, HikariCP 7.0.2입니다.
Spring Boot 4.0.1 BOM은 의존성 버전 관리용이며 HTTP 서버나 Spring 트랜잭션 매니저를 사용하지 않습니다.

## 실습 순서

먼저 [문제](EXERCISES.md)를 읽고 일곱 시나리오를 실행한 다음 [해설](ANSWERS.md)과 비교하세요.

1. 풀은 active=2인데 DB는 idle인 상황
2. 커넥션을 반환한 뒤 후속 작업을 수행하는 상황
3. 연결 획득은 빠르지만 SQL에서 기다리는 상황
4. 잠금 대기가 뒤 요청의 풀 대기로 이어지는 상황
5. 커넥션 획득 timeout
6. statement timeout과 lock timeout
7. 8개 작업에서 풀 크기와 반환 위치 비교

검증은 상태·실행 순서·오류 종류를 검사합니다. 밀리초 수치는 실행 환경마다 달라지며 통과 조건이 아닙니다.
`holdMs`는 획득 직후부터 `close()` 직전까지입니다. 관측 자체의 비용이 있고 모든 실험은 작은 합성 부하입니다.
HTTP 지연, 운영 처리량, p95/p99, 실제 외부 API, Windows/Linux 동작은 검증하지 않았습니다.

[실제 실행 기록과 한계](VERIFICATION.md) / [실행 코드](src/main/java/playground/pool/ConnectionPoolLab.java)
