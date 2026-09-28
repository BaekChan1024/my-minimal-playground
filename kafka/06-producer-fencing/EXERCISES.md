# 예상 → 실행 → 비교

1. old의 send가 성공 응답을 받은 뒤 replacement가 같은 transactional.id로 initTransactions를 완료했습니다. old의 commit과 RC 읽기 결과를 예상하세요.
2. replacement가 같은 key/value를 새 트랜잭션으로 커밋하면 RC/RU에서 몇 건이 보일까요?
3. 두 Producer의 transactional.id는 다르고 client.id만 같다면 이전 Producer의 commit이 거절될까요?

`./play-kafka-06`에서 두 질문에 답을 적고 Enter로 진행하세요. 네 PASS 줄을 예상과 비교하세요.

4. RC에 하나만 남은 이유가 key/value 중복 제거라고 말할 수 있나요? 두 번째 실험의 결과로 설명하세요.
5. ProducerFencedException이 나온 객체로 무조건 abort하고 다시 보내면 되는지 설명하세요.
6. 이 실험이 Consumer Group 재할당 또는 DB 쓰기 배타성까지 검증했나요?

선택 연습: 별도 브랜치에서 두 번째 시나리오의 client.id를 서로 다르게 바꾸고 예상 결과를 비교하세요. 이 변형은 기본 검증 범위에 포함되지 않습니다.
