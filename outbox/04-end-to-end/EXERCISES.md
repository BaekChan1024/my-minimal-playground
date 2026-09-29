# 실행 전 예상

1. posts INSERT 뒤 outbox NOT NULL 위반이면 각각 몇 행이 남을까요?
2. 정상 저장 직후 아직 릴레이를 실행하지 않았다면 Kafka와 Inbox는 몇 건인가요?
3. ACK 뒤 DB 완료 기록 전 실패하고 재전송하면 Kafka·Inbox·projection은 각각 몇 건인가요?
4. Inbox INSERT 뒤 projection DB 쓰기 실패라면 Inbox와 committed offset은 어떻게 되나요?
5. DB 반영 후 offset 커밋 없이 소비자를 다시 열면 기존 Kafka 레코드가 늘어날까요? 업무가 두 번 반영될까요?

조건 변경: consumer-commit-gap에서 첫 소비자 종료 전 offset도 커밋하면 다음 소비자는 무엇을 읽을까요? 현재 테스트가 재수신을 기대한다는 사실과 연결하세요. 복사본에서 수정하고 diff로 되돌리세요.
