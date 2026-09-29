# Spring 05 — OFFSET·keyset 페이지 경계

[상세 글](https://blog.baekchan.com/category/spring)

JDK21과 Git 필요. Spring7.0.2 / PostgreSQL18.4 / JDBC42.7.10. Docker·운영 연결 불필요. 첫 실행에 Maven 의존성과 DB 바이너리 다운로드, loopback 임의 포트에서 임시 DB를 실행하고 종료 시 정리합니다. macOS ARM64 검증, 다른 OS 미검증.

```bash
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach spring-tx-05-v1
./play-spring-05
```

Mac은 `Spring-05.command` 더블클릭. Enter로 일곱 실험 실행/q로 서버 시작 전 종료. 자동 검증은 `./play-spring-05 verify`. 설치된 JDK21을 사용하며 자동 설치하지 않습니다. 기존 작업 변경을 보존한 뒤 태그를 fetch하세요.

PaginationLab은 매 실험마다 1~6 행을 초기화합니다. 페이지마다 별도 읽기 트랜잭션을 끝내고 다음 읽기 전에 auto-commit 쓰기를 확정합니다. 정렬은 published_at DESC NULLS LAST,id DESC. 순서는 sleep/경쟁 스레드에 의존하지 않습니다. 운영 자격증명을 읽지 않습니다.

1. OFFSET 앞 삽입 → 4 중복
2. OFFSET 앞 삭제 → 남아 있는 3 누락
3. Keyset 앞 삽입 → 3,2,1
4. Keyset 경계 행 삭제 → 값으로 3,2,1
5. 같은 날짜 → 날짜 단독 조건의 누락과 복합 키 비교
6. NULL 그룹으로 이동·NULL 경계에서 이어 읽기
7. 아직 읽지 않은 행의 정렬 키 이동 → keyset도 누락

Boundary의 v1|날짜|id를 Base64로 감싸고 NULL 날짜는 ~로 표시합니다. 생성한 cursor의 왕복을 검증합니다. 학습용 규약이며 기존 운영 cursor와 호환되지 않습니다. 서명·만료·필터 결합은 구현하지 않았습니다. HTTP/성능 벤치마크 실험이 아닙니다.

[예상 질문](EXERCISES.md) → 실행 → [해설](ANSWERS.md) · [검증](VERIFICATION.md)
