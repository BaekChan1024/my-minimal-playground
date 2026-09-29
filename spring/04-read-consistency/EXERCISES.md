# 실행 전 예상

1. 같은 트랜잭션의 첫 SELECT 뒤 다른 연결이 수정·커밋하면 두 번째 SELECT는 무엇을 읽을까요? RC와 RR을 비교하세요.
2. 두 번째 find가 같은 객체를 반환했다면 DB에 두 번째 SELECT가 실행됐다는 뜻일까요?
3. 같은 트랜잭션에서 find의 제목은 original인데 JPQL scalar 제목은 updated일 수 있을까요?
4. refresh 또는 clear로 JPA 상태를 바꾸면 DB 스냅샷도 새로 생길까요?
5. readOnly=true에서 count=1을 읽은 뒤 새 행이 커밋되면 이어지는 목록에 두 행이 들어갈 수 있을까요?
6. reader가 RR에서 original을 읽었으면 writer가 롤백된 것일까요? reader 종료 뒤 새 조회로 확인하세요.

조건 변경: independentWrite 호출을 두 번째 조회 뒤로 옮겼을 때 결과를 먼저 예상해 보세요. 기존 assert는 원래 순서를 기대하므로 실패할 수 있습니다. 실습 복사본에서 변경하고 고정 태그의 원본과 비교하세요.
