# Outbox 07 — 보관과 재처리 경계

[상세 글](https://blog.baekchan.com/post/아웃박스-기초-7-outbox와-inbox는-언제-지워도-될까)

JDK21 / 임시 PostgreSQL18.4·Kafka4.1.1 / macOS ARM64 검증. Docker·운영 연결 불필요. 최초 의존성 다운로드 필요. DB 고정 날짜 fixture와 로컬 토픽의 Admin.deleteRecords를 사용합니다. 실제 며칠을 기다린 retention 만료 측정이 아닙니다. 운영 데이터나 사용자 제공 토픽은 받지 않습니다.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach outbox-07-v1
./play-outbox-07
```

Mac은 Outbox-07.command. 자동 검증은 `./play-outbox-07 verify`. Enter 전체 실행/q 서버 시작 전 종료. RetentionLab의 apply와 lesson을 읽습니다.

[예상](EXERCISES.md) → 실행 → [해설](ANSWERS.md) · [검증](VERIFICATION.md).
