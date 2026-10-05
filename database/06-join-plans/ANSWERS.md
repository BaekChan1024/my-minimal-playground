# 실행 후 해설

- narrow는 Nested Loop, customers 3행·orders 반복당100행×3, 최종300행·버퍼14였습니다.
- broad는 Hash Join. customers 1000행 Hash(Batches1,44kB), orders Index Only Scan100000행, 최종100000행·버퍼394였습니다.
- 고객 순 ORDER BY는 Merge Join과 두 Index Only Scan, 별도 Sort 없음, 버퍼392였습니다. 같은 고객 내부의 주문 순서는 계약이 아닙니다.
- 주문 인덱스 제거 후 narrow는 Hash Join·orders Seq Scan100000행·최종300행·버퍼2131이었습니다.
- 첫 두 쿼리는 반환하는 범위가 다릅니다. 버퍼를 지연 시간 배수로 바꿀 수 없습니다.

이는 자동 실습의 관측이며 사용자 숙지 확인을 대신하지 않습니다.
