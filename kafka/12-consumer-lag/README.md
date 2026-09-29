# Kafka 12 — Consumer lag와 처리 병목

상세 글: [Kafka 연재](https://blog.baekchan.com/category/kafka) (발행 후 직접 링크 연결)

JDK21과 Git. 실제 임시 브로커1·컨트롤러1이며 외부 Kafka/DB/Docker는 필요 없습니다. 그룹 합류와 여섯 비교를 위해 수십 초 걸릴 수 있습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach kafka-12-v1
./play-kafka-12
```

기존 변경을 보존하고 태그를 fetch하세요. Mac `Kafka-12.command`, 자동 `./play-kafka-12 verify`. Enter로 시작, q로 시작 전 종료. 임시 데이터는 종료 시 정리됩니다. Windows: `gradlew.bat :kafka:12-consumer-lag:run --args=guided --console=plain` (Windows 미실행).

첫 비교는 assign으로 position·commit 경계를 분리합니다. 후속 6조건은 실제 subscribe/classic/RangeAssignor/파티션 2개이며 Consumer별 전용 스레드입니다. 모든 그룹 할당이 안정되고 pause한 뒤 부하를 시작합니다. burst는 입력 160건 완료 후 소비 시작, paced는 소비 시작 후 약 12ms 간격으로 입력. 업무는 건당 10/20ms sleep인 합성 지연입니다. 실제 DB/HTTP 성능이나 운영 부하 재현은 아닙니다.

CSV의 lag는 Admin 끝 오프셋−그룹 커밋입니다. burst는 소비 시작 약 250ms 뒤, paced는 유입 종료 직후 관측합니다. 최대 처리율 보장이나 고정 실행시간을 통과 조건으로 사용하지 않습니다.

[문제](EXERCISES.md) · [해설](ANSWERS.md) · [검증 및 CSV](VERIFICATION.md)
