# Spring 01 — 트랜잭션 호출·예외·전파 경계

상세 글: [Spring 연재](https://blog.baekchan.com/category/spring) (발행 후 직접 링크 연결)

JDK 21과 Git이 필요합니다. 실제 임시 PostgreSQL 18.4와 Spring Framework 7.0.2를 사용합니다. Docker나 기존 DB는 필요 없습니다. 최초 실행 때 Maven Central에서 의존성과 실습용 PostgreSQL 바이너리를 받습니다. 임시 서버는 loopback 임의 포트만 사용하며 종료 시 정리합니다. 운영 설정·자격증명을 읽지 않습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach spring-tx-01-v1
./play-spring-01
```

기존 저장소에서는 작업 중인 변경을 보존하고 태그를 fetch하세요. Mac에서는 `Spring-01.command`를 더블클릭할 수 있습니다. Enter로 진행, q로 서버 시작 전에 종료합니다. 자동 검증은 `./play-spring-01 verify`입니다. 설치된 JDK21을 찾으며 자동 설치하지 않습니다. macOS ARM64에서 실행 검증했으며 다른 OS는 미검증입니다.

## 실험 구성

`TransactionLab.java`에 실제 Spring Bean 두 개와 JDBC 저장 도우미가 있습니다. `@EnableTransactionManagement`로 프록시를 만들고 외부 실행기가 Bean을 호출합니다. 실행기는 트랜잭션을 소유하지 않습니다. 서비스 호출이 끝난 뒤 세 테이블의 행 수를 다시 읽습니다.

| 시나리오 | 확인할 질문 |
|---|---|
| external-runtime | 외부 프록시 호출의 RuntimeException은 무엇을 되돌리는가? |
| checked-default | IOException이 발생해도 기본 규칙에서는 저장이 남는가? |
| checked-rollbackFor | 명시적인 rollbackFor는 결과를 바꾸는가? |
| self-invocation | 바깥 트랜잭션이 없을 때 this 호출은 새 경계를 만드는가? |
| caught-local | 같은 메서드에서 Java 예외를 삼키면 무슨 일이 생기는가? |
| caught-required | 안쪽 REQUIRED 프록시를 지난 예외를 잡아도 커밋할 수 있는가? |
| requires-new-audit | 안쪽 커밋 뒤 바깥이 실패하면 어떤 기록이 남는가? |

테이블은 `post_record`, `outbox_record`, `audit_record`입니다. 시도 기록은 별도 트랜잭션 비교용 예시입니다. DB 트랜잭션 식별자는 같은지/다른지만 비교해 출력합니다. Kafka·JPA·HTTP 서버는 띄우지 않습니다.

[예상 질문](EXERCISES.md) → 실행 → [해설](ANSWERS.md) · [검증 기록](VERIFICATION.md)
