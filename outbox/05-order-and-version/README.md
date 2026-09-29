# Outbox 05 — 이벤트 순서와 버전

[상세 글](https://blog.baekchan.com/post/아웃박스-기초-5-중복은-막았는데-글이-과거로-돌아간다면)

JDK 21 / 실제 임시 PostgreSQL 18.4. macOS ARM64 검증. Kafka는 시작하지 않고 소비 함수에 역순 입력을 직접 전달합니다. Docker·운영 DB 불필요, 최초 의존성 다운로드 필요.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach outbox-05-v1
./play-outbox-05
```

Mac은 Outbox-05.command. 자동 검증은 `./play-outbox-05 verify`. Enter로 전체 실행, q로 서버 시작 전 종료. 읽을 코드는 OrderLab.apply와 scenario.

[예상](EXERCISES.md) → 실행 → [해설](ANSWERS.md) · [검증](VERIFICATION.md).
