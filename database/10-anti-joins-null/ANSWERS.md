# 실행 후 해설

1. 고객3만 반환한다. 고객1은 주문2개, 고객2는 주문1개를 가진다.
2. NOT IN은 0행, NOT EXISTS와 LEFT JOIN은 고객3을 반환한다. 내부 NULL을 걸러낸 NOT IN도 고객3을 반환한다.
3. 1 NOT IN(1,2,NULL)은 false, 3의 경우 NULL이다. 외부 NULL은 NULL을 제거한 비어 있지 않은 목록에서도 NOT IN 결과가 NULL이다. 등호 조건의 NOT EXISTS는 true다. 빈 목록에서는 외부 NULL의 NOT IN도 true다.
4. 실제 주문의 note가 NULL인 고객1이 주문 없는 고객3과 함께 나온다. 실제 일치 행에서 NULL일 수 없는 주문 기본 키를 검사하면 고객3만 남는다. 이번 등호 조건에서는 o.customer_id IS NULL도 안전하다.
5. NOT EXISTS와 LEFT JOIN은 Hash Anti Join, NULL 제거 NOT IN은 hashed SubPlan을 포함한 Seq Scan이었다. 모두 고객1001부터1010까지를 정확히 반환했고 버퍼 접근은2,133이었다.
6. NOT EXISTS/LEFT JOIN은 Nested Loop Anti Join의 반복 탐색으로2,026, NULL 제거 NOT IN은 인덱스100,000행을 읽는 hashed SubPlan으로91이었다. 내부 Actual Rows0.99는1,010회 반복의 반올림된 평균이다. 이 수치로 시간 우열을 주장하지 않는다.
7. NULL 없이 다시 적재하고 제약을 적용하자 셋 다 같은10명을 반환했다. NOT IN은 여전히 hashed SubPlan, 나머지는 Nested Loop Anti Join이었다. 재적재가 인덱스 물리 상태도 바꾸므로 버퍼 차이를 제약 효과로 보지 않는다.

이는 자동 실습의 관측이며 사용자 숙지 확인을 대신하지 않습니다.
