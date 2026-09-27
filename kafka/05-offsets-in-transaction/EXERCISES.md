# 예상 → 실행 → 비교

1. 별도 커밋 경로에서 첫 출력 뒤 Consumer를 닫으면 다음 Consumer의 시작 위치는 무엇일까요? position과 committed를 구분해 보세요.
2. sendOffsetsToTransaction 호출 뒤 abort하면 입력 그룹 커밋과 RC 출력은 각각 어떻게 될까요?
3. abort 후 새 Consumer로 재처리해 commit하면 처리 시도 수와 RC 출력 수가 같을까요?
4. 성공한 commit 뒤 다시 Consumer를 열었을 때 입력이 재전달되지 않았다는 사실을 어떤 관측으로 확인해야 할까요?

`./play-kafka-05`를 실행하고 Enter 두 번으로 비교하세요. 다섯 PASS 줄의 inputCommitted, outputRC, outputRU, attempts, position을 예상과 대조하세요.

5. 입력 offset 0을 읽었는데 nextOffsets가 1인 이유는 무엇인가요? 출력 오프셋을 대신 넣으면 안 되는 이유도 설명하세요.
6. abort가 Consumer 객체의 현재 position을 되돌려 주나요? 이 실습의 close/reopen과 같은 객체를 유지하는 루프는 어떻게 다른가요?
7. 최종 RC 출력이 한 건이면 DB 변경이나 결제 호출도 한 번이었다고 말할 수 있나요?

선택 연습: 별도 작업 브랜치에서 별도 커밋 경로의 두 번째 출력 전송을 생략하고 기대 목록도 수정해 보세요. 이는 Kafka가 중복을 제거한 것인지, 애플리케이션이 전송 자체를 하지 않은 것인지 구분하세요. 이 변형은 기본 검증 범위에 포함되지 않습니다.
