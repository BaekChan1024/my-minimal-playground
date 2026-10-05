# my-minimal-playground

개념과 상세 해설은 **[개발 블로그](https://blog.baekchan.com)**의 [Kafka](https://blog.baekchan.com/category/kafka)·[Spring](https://blog.baekchan.com/category/spring) 연재에서 읽을 수 있습니다.

블로그의 설명을 직접 실행하고, 조건을 바꾸며 확인하는 공개 학습 저장소입니다.
글 전문은 블로그에 발행하고, 이 저장소에는 실행 코드·실습 문제·실행 후 해설·검증 기록을 제공합니다.
한 회차의 코드와 글을 완성한 뒤 실습하고, 원리를 설명할 수 있게 되면 다음 회차로 넘어갑니다.

## Kafka 실습

| 회차 | 주제 | 실행 안내 | 글 |
|---|---|---|---|
| Kafka 01 | 로그·오프셋·커밋·재소비 | [실습 README](kafka/01-log-and-offset/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-첫-실습-읽은-메시지는-사라질까-로그오프셋커밋-이해하기) |
| Kafka 02 | 파티션·키·Consumer Group·재할당 | [실습 README](kafka/02-partitions-and-groups/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-consumer를-늘리면-더-빨라질까-파티션키consumer-group-이해하기) |
| Kafka 03 | poll 제한·재전달·뒤늦은 커밋 거절 | [실습 README](kafka/03-poll-timeout-and-commit/README.md) | [블로그 글](https://blog.baekchan.com/post/kafka-처리를-끝냈는데-왜-다시-읽힐까-poll-제한리밸런스커밋-경계) |
| Kafka 04 | 트랜잭션·읽기 격리·abort·중복 send | [실습 README](kafka/04-transactions-and-visibility/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-트랜잭션이-롤백하는-것은-어디까지일까-abortreadcommittedlso) |
| Kafka 05 | 입력 오프셋·출력의 트랜잭션 결합 | [실습 README](kafka/05-offsets-in-transaction/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-입력-오프셋과-출력-메시지를-함께-커밋하기-sendoffsetstotransaction-실습) |
| Kafka 06 | transactional.id·Producer fencing·소유자 인계 | [실습 README](kafka/06-producer-fencing/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-새-producer가-시작되면-이전-producer는-어떻게-될까-transactionalid와-fencing) |
| Kafka 07 | Kafka 전송 성공은 어디까지 안전할까? 복제·ISR·acks 실습 | [실습 README](kafka/07-replication-isr/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-전송-성공은-어디까지-안전할까-복제isracks-실습) |
| Kafka 08 | Kafka 재시도는 어디까지 중복을 막을까? 멱등 전송·업무 중복·순서 | [실습 README](kafka/08-idempotence-retries/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-재시도는-어디까지-중복을-막을까-멱등-전송업무-중복순서) |
| Kafka 09 | Kafka 메시지는 언제 삭제될까? 로그 세그먼트·retention·사라진 오프셋 | [실습 README](kafka/09-retention-segments/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-메시지는-언제-삭제될까-로그-세그먼트retention사라진-오프셋) |
| Kafka 10 | Kafka는 같은 key의 과거 값을 언제 지울까? Log Compaction·tombstone 실습 | [실습 README](kafka/10-log-compaction/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka는-같은-key의-과거-값을-언제-지울까-log-compactiontombstone-실습) |
| Kafka 11 | Kafka 전송은 무엇을 기다릴까? batch·linger·압축과 지연 측정 | [실습 README](kafka/11-batching-compression/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-전송은-무엇을-기다릴까-batchlinger압축과-지연-측정) |
| Kafka 12 | Kafka lag가 늘면 Consumer부터 늘릴까? 처리율·파티션 쏠림·커밋 진단 | [실습 README](kafka/12-consumer-lag/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/kafka-lag가-늘면-consumer부터-늘릴까-처리율파티션-쏠림커밋-진단) |

Kafka 06~12는 각 회차의 `./play-kafka-NN` 또는 Mac의 `Kafka-NN.command`로 실행합니다. 예: `./play-kafka-12`. 자동 검증은 뒤에 `verify`를 붙이며, 각 고정 버전은 `kafka-NN-v1`입니다. 회차별 준비물·예상 질문·관측 결과는 위 실습 README에서 확인하세요.

Kafka 03은 `./play-kafka-03` 또는 `Kafka-03.command`로 실행합니다. 자동 검증은 `./play-kafka-03 verify`, 고정 버전은 `kafka-03-v1`입니다.

Kafka 04는 `./play-kafka-04` 또는 `Kafka-04.command`로 실행합니다. 자동 검증은 `./play-kafka-04 verify`, 고정 버전은 `kafka-04-v1`입니다.

Kafka 05는 `./play-kafka-05` 또는 `Kafka-05.command`로 실행합니다. 자동 검증은 `./play-kafka-05 verify`, 고정 버전은 `kafka-05-v1`입니다.

position·seek·commit·ack의 관계는 [보충 글](https://blog.baekchan.com/post/kafka의-positionseekcommitack-읽은-위치와-처리-완료는-어떻게-다를까)에서 이어서 설명합니다.

Kafka 02는 `./play-kafka-02` 또는 Mac의 `Kafka-02.command`로 시작합니다.
`./play-kafka-02 verify`는 7단계 실험을 자동 실행합니다. 고정 버전은 `kafka-02-v1.1`입니다.
기존 `./play`와 `Kafka-01.command`는 Kafka 01을 그대로 실행합니다.

## Spring 실습

| 회차 | 주제 | 실행 안내 | 글 |
|---|---|---|---|
| Spring 01 | 트랜잭션 프록시·롤백 규칙·전파 | [실습 README](spring/01-transaction-boundaries/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/spring-transactional은-언제-롤백할까-프록시예외전파를-db로-확인하기) |
| Spring 02 | JPA save·flush·commit·실패 시점 | [실습 README](spring/02-jpa-flush-commit/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/jpa-save와-flush는-언제-저장될까-sql-실행커밋실패-시점-이해하기) |
| Spring 03 | 동시 수정·낙관적 잠금·편집 버전 충돌 | [실습 README](spring/03-concurrent-editing/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/같은-글을-두-사람이-수정하면-jpa-낙관적-잠금버전-충돌행-잠금-실습) |
| Spring 04 | 조회 일관성·격리 수준·JPA 1차 캐시 | [실습 README](spring/04-read-consistency/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/같은-트랜잭션인데-조회-값이-다를까-readonly격리-수준jpa-1차-캐시) |
| Spring 05 | OFFSET·Keyset·페이지 경계 | [실습 README](spring/05-pagination/README.md) | [블로그에서 읽기](https://blog.baekchan.com/post/cursor가-있는데-글이-중복될까-offsetkeyset페이지-경계-실습) |

`./play-spring-01` 또는 Mac의 `Spring-01.command`로 시작합니다. 고정 버전은 `spring-tx-01-v1`이며 실제 임시 PostgreSQL을 사용합니다. Spring 02는 `./play-spring-02` 또는 `Spring-02.command`, 고정 버전 `spring-tx-02-v1`로 실행합니다. Spring 03은 `./play-spring-03` 또는 `Spring-03.command`, 고정 버전 `spring-tx-03-v1`입니다. Spring 04는 `./play-spring-04` 또는 `Spring-04.command`, 고정 버전 `spring-tx-04-v1`입니다. Spring 05는 `./play-spring-05` 또는 `Spring-05.command`, 고정 버전 `spring-tx-05-v1`입니다.

## 서비스 복구 실습

[전환 후 데이터 복구 실습](operations/01-cutover-recovery/README.md)은 새 글·수정·삭제·파일·복구 후 다음 쓰기를 확인합니다. `./play-recovery-01` 또는 Mac의 `Recovery-01.command`로 실행하고, 고정 버전은 `recovery-01-v1`입니다. 설명은 [블로그 글](https://blog.baekchan.com/post/서비스-전환-후-롤백하면-새로-쓴-글은-어디로-갈까)에서 읽을 수 있습니다.

## 가장 쉬운 실행 (Mac · Kafka 01)

저장소 폴더에서 **`Kafka-01.command`를 더블클릭**하고 `1`을 선택하세요.
설치된 JDK 21을 자동으로 찾으며, 읽을 개수를 선택한 다음 Enter로 6단계 실습을 진행합니다.
각 단계는 실행 전에 예상할 질문을 보여 줍니다. 중간에 `q`를 입력하면 종료합니다.

| 선택 | 하는 일 |
|---|---|
| 1 (기본) | 설명과 질문을 읽고 Enter로 실험 진행 |
| 2 | 기존 `seed`, `read`, `seek` 명령으로 자유 실습 |
| 3 | 실제 Kafka에서 7개 시나리오 자동 검증 |
| 0 | 실행하지 않고 종료 |

터미널에서는 저장소 루트에서 `./play`만 입력해도 같은 메뉴가 나옵니다.

## 처음 저장소를 받는 경우

준비물: **JDK 21**, Git, 최초 의존성 다운로드를 위한 인터넷 연결.
Gradle Wrapper가 포함되어 있습니다. Docker·별도 Kafka·DB 설치는 필요 없습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-02-v1.1
./play
```

Mac의 더블클릭 실행 도구와 `./play`는 설치된 JDK를 확인하며 자동 설치하거나 시스템 설정을 변경하지 않습니다.
JDK 21이 없으면 설치 안내를 표시합니다. 최초 다운로드 이후에도 실행 준비 시간은 필요합니다.

Windows에서는 JDK 21을 준비한 뒤 `gradlew.bat :kafka:01-log-and-offset:run --args=guided --console=plain`을 실행합니다.
검증 환경은 macOS ARM64 / JDK 21입니다. Windows·Linux에서의 실행 결과는 아직 확인하지 않았습니다.

자유 실습(메뉴 2)을 선택해 `lab>`가 나타나면 [실습 문제](kafka/01-log-and-offset/EXERCISES.md)를 열고 진행하세요.

```text
seed
read study 2 no-commit
status study
quit
```

자동 검증만 실행하려면:

```bash
./play verify
```

각 실행은 임시 데이터와 임의의 로컬 포트를 사용하는 실제 Kafka 브로커를 만듭니다.
정상 종료하면 브로커와 실습 데이터가 정리되며, 다음 실행은 빈 상태로 시작합니다.
공유 서버 접속 정보나 비밀값은 필요 없습니다.

`kafka-01-v1` 태그는 최초 글의 명령어 실습을 그대로 보존합니다. 쉬운 실행 도구는 `kafka-01-v1.1`부터 포함됩니다.

Kafka 06은 `./play-kafka-06` 또는 `Kafka-06.command`로 실행합니다. 자동 검증은 `./play-kafka-06 verify`, 고정 버전은 `kafka-06-v1`입니다.

## JWT 검증 실습

[모의 인증과 실제 JWT 검증](security/01-jwt-validation/README.md)은 decoder 호출, 서명·claim 정책과 API 권한을 비교합니다. `./play-security-01` 또는 Mac의 `Security-01.command`, 고정 버전 `security-jwt-01-v1`로 실행합니다. 전체 해설은 [인증 테스트가 통과하면 JWT 검증도 끝난 걸까?](https://blog.baekchan.com/post/인증-테스트가-통과하면-jwt-검증도-끝난-걸까)에서 읽을 수 있습니다.

## 이미지 업로드 검증 실습

[이미지 검증 경계](security/02-image-boundaries/README.md)는 PNG 시그니처·디코딩과 SVG 문서·이미지 문맥을 비교합니다. `./play-image-01`, 브라우저 비교는 `./play-image-01 serve`로 실행합니다. 고정 태그는 `image-boundaries-01-v1`이며 전체 설명은 [블로그 글](https://blog.baekchan.com/post/이미지-업로드-성공은-어디까지-검증한-걸까)에서 읽을 수 있습니다.

## 부분 문자열 검색 실습

[검색어와 검색 인덱스](search/01-substring-search/README.md)는 짧은 한국어·긴 검색어, list/count와 정렬 비용을 비교합니다. `./play-search-01` 또는 `Search-01.command`로 실행합니다. 고정 태그는 `search-substring-01-v1`이며 글 링크는 발행 후 연결합니다.
