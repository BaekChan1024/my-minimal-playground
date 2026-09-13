# Kafka 01 검증 기록

검증일: 2026-09-13. 실행 환경: macOS ARM64, Temurin JDK 21.0.6, Gradle 9.2.1.
Kafka broker/client artifacts 4.1.1, spring-kafka-test 4.0.1.
정확한 전이 의존성은 함께 커밋한 gradle.lockfile을 참고합니다.

## 자동 검증

```bash
./gradlew :kafka:01-log-and-offset:verifyLab --console=plain
```

실제 로컬 Kafka와 일반 KafkaConsumer/KafkaProducer를 사용하여 다음 결과를 확인했습니다.
출력에서 Gradle 안내와 임시 디렉터리 경로는 제외했습니다.

```text
Kafka 4.1.1 | 1 broker | 1 partition | temporary data
stored value=A partition=0 offset=0
stored value=B partition=0 offset=1
stored value=C partition=0 offset=2
result group=study read=[0, 1] position=2 committed=null
PASS: read without commit
result group=study read=[0, 1] position=2 committed=2
PASS: reopen and commit
result group=study read=[2] position=3 committed=3
PASS: resume even with earliest
result group=study read=[] position=3 committed=3
PASS: caught up
result group=audit read=[0, 1, 2] position=3 committed=3
PASS: independent group
status group=study start=0 end=3 committed=3 lag=0
result group=study read=[0, 1, 2] position=3 committed=3
PASS: seek without changing commit
result group=study read=[] position=3 committed=3
PASS: seek was only local
PASS: 7 consumer scenarios; log boundaries remain [0, 3).
BUILD SUCCESSFUL
```

검증은 반환 오프셋·현재 위치·커밋의 일치를 assertion으로 확인합니다.
단순히 콘솔에 PASS를 무조건 출력하는 프로그램이 아닙니다.

## 대화형 경로와 조건 변경

별도 `run` 실행에서 입력한 명령:

```text
seed
status study
read study 1 no-commit
read study 1 commit
read study 1 no-commit
status study
seed
read study 0 no-commit
quit
```

관측 결과:

- 초기 커밋 없음: `committed=none lag=unknown`.
- 첫 read: `[0] position=1 committed=null`.
- 두 번째 read: `[0] position=1 committed=1`.
- 세 번째 read: `[1] position=2 committed=1`.
- 상태: `start=0 end=3 committed=1 lag=2`.
- 중복 seed를 거부했고, COUNT 0도 안내 문구와 함께 거부했습니다.
- quit 후 정상 종료했습니다. 자동 검증 실행과 대화형 실행은 서로 다른 임시 브로커에서 offset 0부터 시작했습니다.

## 범위와 한계

- 파티션 1개를 assign으로 직접 할당. 그룹 ID별 커밋 저장만 검증했습니다.
- subscribe 기반 그룹 분배·리밸런스, 복제 내구성, 프로세스 강제 종료, 브로커 재시작, 실제 DB 부작용은 검증하지 않았습니다.
- 레코드 보존 기간 만료나 로그 압축을 실행한 실험이 아닙니다.
- 처리량·지연 벤치마크가 아닙니다. Gradle 실행 시간을 Kafka 성능으로 해석하지 않습니다.
- macOS 이외 운영체제에서의 검증은 아직 없습니다.
- 위 실험은 AI가 실행했습니다. 사용자의 실습 완료나 숙지 완료를 의미하지 않습니다.
