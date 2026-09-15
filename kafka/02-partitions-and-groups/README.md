# Kafka 02 — 파티션·키·Consumer Group

Consumer를 1→2→3→4개로 늘려 파티션 3개의 담당자가 어떻게 바뀌는지 관찰합니다.
실제 Kafka를 사용하며, 숫자를 미리 정해 출력하는 시뮬레이터가 아닙니다.

- [상세 글](ARTICLE.md)
- [실행 전 예상](EXERCISES.md)
- [실행 후 해설](ANSWERS.md)
- [검증 기록](VERIFICATION.md)
- [실제 Consumer 코드](src/main/java/playground/kafka/GroupLab.java)

## 실행

JDK 21을 설치하고 저장소 루트에서 실행합니다. Docker와 별도 서버는 필요 없습니다.
처음 실행에는 Gradle 및 의존성 다운로드가 필요합니다.

```bash
./play-kafka-02          # 질문을 읽고 Enter로 진행, q로 종료
./play-kafka-02 verify   # 동일 실험의 자동 실행과 검증
```

Mac에서는 `Kafka-02.command`를 열어 같은 안내 모드를 실행할 수 있습니다.
실행 도구는 설치된 JDK 21을 찾으며 자동 설치나 시스템 설정 변경은 하지 않습니다.
Windows의 대응 명령은 아래와 같으며, Windows 실행 검증은 하지 않았습니다.

```text
gradlew.bat :kafka:02-partitions-and-groups:run --args=guided --console=plain
```

고정 태그 `kafka-02-v1`을 사용합니다. 기존 저장소에 수정 중인 파일이 있다면 보존하세요.
이미 예전 태그에 머물러 있다면 원격 태그를 받아 `kafka-02-v1`을 선택해야 새 도구가 보입니다.

## 일곱 단계

1. 파티션 3개를 Consumer 1개가 담당.
2. 두 번째 Consumer 합류 후 2개/1개 분배.
3. 세 번째 Consumer 합류 후 각각 1개.
4. 네 번째 Consumer 합류 후 하나는 할당 없음.
5. audit 그룹이 study 그룹이 읽은 36개를 독립적으로 읽음.
6. 할당이 있던 Consumer 한 개를 정상 종료한 뒤 남은 3개가 재개.
7. 같은 키로 9개를 보내 한 Consumer로 부하가 집중되는지 확인.

각 생산 단계는 9개이며, 전체 로그는 54개입니다. 두 그룹 각각 54개를 관측합니다.
`assigned`/`revoked`는 Consumer 콜백, `assignment`는 Admin과 로컬 할당을 대조한 안정 상태입니다.
`processed`는 해당 단계의 관측 건수입니다. 실제 DB 업무는 없습니다.

## 실험 조건

- Kafka broker/client 의존성 4.1.1, spring-kafka-test 4.0.1, Gradle 9.2.1, JDK 21.
- 브로커 1개, 토픽 1개, 파티션 3개, 복제 계수 1. 임시 디렉터리와 로컬 포트 사용.
- 그룹 프로토콜 classic, RangeAssignor, subscribe 기반 동적 할당.
- Consumer마다 전용 스레드. 안내 입력을 기다리는 동안에도 poll은 계속 실행됨.
- UTF-8 문자열 키, 기본 Java Producer 분배, acks=all, idempotence=true.
- 각 파티션으로 가는 합성 키를 의도적으로 골라 초기 데이터를 균등하게 만듦.
- 자동 커밋 비활성, poll 결과를 관측으로 구성한 후 nextOffsets를 동기 커밋.
- 각 단계의 처리가 끝나고 커밋된 다음 멤버 변경. 재할당 안정 상태 확인 후 다음 데이터 전송.

안정 상태를 최대 45초, 읽기를 최대 30초 기다리며 실패하면 종료 코드가 0이 아닙니다.
코드에는 레코드 식별자·파티션·오프셋·키별 순서·그룹별 커밋의 비교가 포함되어 있습니다.
정상 종료 또는 q 입력 시 Consumer와 임시 브로커를 정리합니다. 다음 실행은 새 실험입니다.

처리량·지연·운영 장애 복구를 측정한 실험은 아닙니다. 순차 커밋 후 정상 종료만 수행하며,
강제 종료·미완료 업무·복제 장애·새 consumer 프로토콜·협력적 재할당은 검증하지 않습니다.
