# Outbox 04 — 저장부터 Kafka·Inbox까지

[상세 글](https://blog.baekchan.com/post/아웃박스-기초-4-글-저장부터-kafka와-inbox까지-직접-따라가기)

JDK 21과 Git으로 실행합니다. Docker·운영 연결 정보는 필요 없습니다. 최초 실행은 Maven Central 다운로드가 필요합니다. 임시 PostgreSQL 18.4와 Kafka 4.1.1을 시작하고 정리합니다. macOS ARM64 검증, 다른 OS 미검증.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach outbox-04-v1
./play-outbox-04
```

Mac: `Outbox-04.command`. 자동 검증: `./play-outbox-04 verify`. 안내 모드는 Enter로 전체 실험을 시작하고 q로 시작 전에 종료합니다. 시나리오별 대화형 제어는 아닙니다.

읽을 코드: `OutboxLab.java`의 save → relay → apply → commit. JDBC와 Kafka API를 직접 사용하며 Spring listener를 실행하지 않습니다. 각 시나리오마다 DB 초기화·새 토픽을 사용합니다.

[먼저 예상](EXERCISES.md) → 실행 → [해설](ANSWERS.md) · [검증](VERIFICATION.md).
