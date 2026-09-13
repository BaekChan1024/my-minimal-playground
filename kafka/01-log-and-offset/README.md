# Kafka 01 — 읽은 메시지는 어디에 남아 있을까?

공개 블로그: [Kafka 첫 실습 — 로그·오프셋·커밋 이해하기](https://blog.baekchan.com/post/kafka-첫-실습-읽은-메시지는-사라질까-로그오프셋커밋-이해하기)

이번 실습에서는 Kafka 레코드의 위치, Consumer의 현재 위치, 그룹에 저장한 재시작 위치를 구분합니다.

- [상세 글](ARTICLE.md)
- [먼저 풀어 볼 실습 문제](EXERCISES.md)
- [실행 후 확인할 해설](ANSWERS.md)
- [검증 기록](VERIFICATION.md)
- [핵심 Java 코드](src/main/java/playground/kafka/OffsetLab.java)

## 가장 쉬운 시작

Mac에서는 저장소 루트의 **`Kafka-01.command`를 더블클릭 → 1번 선택 → Enter로 단계 진행**하면 됩니다.
읽을 개수는 1~3 중에서 고르고, 도중에 `q`를 입력하면 브로커를 정리하고 종료합니다.
터미널에서는 저장소 루트에서 `./play`를 실행하세요. JDK 21은 자동으로 찾아 사용합니다.

```bash
./play          # 번호 메뉴
./play guided   # 질문과 함께 6단계 실습
./play shell    # 기존 명령어 실습
./play verify   # 7개 자동 검증 시나리오
```

안내 모드도 아래 자유 실습과 동일한 read·commit·seek 로직 및 실제 Kafka를 사용합니다.
프로그램이 결과를 대신 출력하더라도 사용자가 이해했다고 자동 판정하지 않습니다.
이 도구는 `kafka-01-v1.1`부터 제공하며, 최초 글의 `kafka-01-v1` 태그에는 포함되지 않습니다.

## 환경과 직접 실행

| 항목 | 버전·설정 |
|---|---|
| JDK | 21 |
| Gradle Wrapper | 9.2.1 |
| Kafka broker/client artifacts | 4.1.1 |
| 브로커 시작 도구 | spring-kafka-test 4.0.1 |
| 애플리케이션 | 일반 Java main, Spring Boot 서버 없음 |
| 클러스터 | KRaft, broker 1개, partition 1개, replication factor 1 |
| 데이터 | `A`, `B`, `C` 세 레코드, 동일 키 `post-42` |
| 자동 커밋 | false |
| 시작 위치 보완 정책 | earliest |
| 한 번의 poll 반환 상한 | max.poll.records=1 |
| 보존 설정 | log.retention.hours=24; 실행 종료 시 실습 환경 전체 정리 |

저장소 루트에서 실행합니다.

```bash
java -version
./gradlew :kafka:01-log-and-offset:run --console=plain
```

`JAVA_HOME`이 JDK 21을 가리키는지 확인하세요. macOS에서 필요하면
`export JAVA_HOME=$(/usr/libexec/java_home -v 21)`로 설정할 수 있습니다.
처음 실행할 때는 Gradle과 Kafka 관련 의존성 다운로드 시간이 필요합니다.
JVM 최대 힙 설정은 Gradle 512MiB, 실습 프로세스 768MiB입니다. 실제 전체 메모리는 힙보다 클 수 있으므로 여유 메모리를 확보하세요.

프로그램은 실제 브로커를 시작하지만 테스트용 라이브러리로 수명주기를 관리합니다.
프로덕션 Kafka 배포 템플릿으로 사용하기 위한 구성은 아닙니다.

## 명령어

`lab>` 프롬프트에 입력합니다. 각 `read`·`seek` 명령은 **새 Consumer를 만들어 읽고 닫습니다**.
브로커는 `quit`까지 살아 있으므로 명령 사이에 레코드와 커밋이 유지됩니다.

| 명령 | 의미 |
|---|---|
| `seed` | 빈 토픽에 A, B, C를 한 번 저장 |
| `read study 2 no-commit` | study의 저장 위치에서 최대 2개 읽고 커밋하지 않음 |
| `read study 2 commit` | 최대 2개 읽으며 각 출력 직후 다음 오프셋을 커밋 |
| `status study` | 로그 시작·끝, 그룹 커밋, 오프셋 차이로 계산한 lag 확인 |
| `seek study 0 3` | 현재 Consumer만 offset 0으로 이동하고 최대 3개 읽음; 커밋하지 않음 |
| `help` | 명령 안내 |
| `quit` | 브로커 종료·실습 데이터 정리 |

`COUNT`는 1~100이며 존재하는 레코드까지만 읽습니다. 새 이벤트를 계속 기다리는 실시간 구독 명령이 아닙니다.
`seed`는 중복 입력을 거부합니다. 처음부터 다시 하려면 `quit` 후 실행 명령을 다시 입력합니다.

로그 수준은 ERROR로 제한했습니다. 상세 Kafka 로그를 보려면 `src/main/resources/logback.xml`의 root level을 INFO로 바꿀 수 있습니다.

## 이번 실습에서 의도적으로 단순화한 부분

`consumer.assign()`으로 partition 0을 직접 할당합니다. 그룹별 오프셋 저장은 사용하지만,
`subscribe()`를 통한 그룹 가입·파티션 분배·리밸런스는 실험하지 않습니다.
동일 그룹의 여러 Consumer를 동시에 실행하는 예제로 확장하려면 이 차이를 먼저 이해해야 합니다.

처리는 콘솔 출력입니다. DB 저장·업무 트랜잭션·프로세스 강제 종료·브로커 재시작·보존 기간 만료·복제 내구성은 이번 실습의 검증 범위에 포함하지 않습니다.

## 자동 검증

```bash
./gradlew :kafka:01-log-and-offset:verifyLab --console=plain
```

7개 소비 시나리오에서 반환 오프셋, 현재 position, 저장된 committed offset을 검증하고 로그 경계도 확인합니다.
불일치하면 프로세스가 실패하며 Gradle 작업도 실패합니다. `check`에도 연결되어 있습니다.
정확한 의존성 버전은 `gradle.lockfile`에 고정했습니다. 일반 학습 실행에는 `--write-locks`를 쓰지 않습니다.
