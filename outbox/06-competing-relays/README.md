# Outbox 06 — 경쟁하는 릴레이

[상세 글](https://blog.baekchan.com/post/아웃박스-기초-6-릴레이를-두-개-실행하면-어떻게-될까)

JDK21, 임시 PostgreSQL18.4·Kafka4.1.1. macOS ARM64 검증, Docker·운영 연결 불필요. 최초 의존성 다운로드 필요. DB 연결 두 개의 트랜잭션을 실제로 겹치고 호출 순서를 통제합니다. 별도 OS 프로세스 두 개나 처리량 시험이 아닙니다. 실제 Kafka 레코드의 key/value/offset 순서를 검사합니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach outbox-06-v1
./play-outbox-06
```

Mac: Outbox-06.command. 자동 검증: `./play-outbox-06 verify`. Enter 전체 실행/q 시작 전 종료. 읽을 코드: RelayLab의 take, guard, scenarios.

[예상](EXERCISES.md) → 실행 → [해설](ANSWERS.md) · [검증](VERIFICATION.md).
