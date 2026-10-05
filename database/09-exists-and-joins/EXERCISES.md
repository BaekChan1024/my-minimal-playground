# 먼저 예상하고 직접 비교하기

1. 주문 5만 행이 조건에 맞으면 COUNT(*) > 0과 EXISTS는 같은 양을 읽을까?
2. 존재하지 않는 customer_id=2000을 찾으면 인덱스 유무가 어떻게 영향을 줄까?
3. 고객마다 주문이 100개라면 고객 ID만 JOIN한 결과는 몇 행일까?
4. DISTINCT + JOIN과 EXISTS가 같은 고객 목록을 반환하면 버퍼 접근도 같을까?
5. EXISTS가 항상 Nested Loop Semi Join으로 실행될까?
6. EXISTS(SELECT count(*) FROM orders WHERE customer_id=2000)은 어떤 값을 반환할까?
