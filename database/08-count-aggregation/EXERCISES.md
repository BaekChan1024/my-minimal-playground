# 먼저 예상하고 직접 비교하기

1. Aggregate Actual Rows=1과 COUNT 값100000은 무엇이 다른가요?
2. COUNT(*)와 COUNT(amount)의 결과가 다른 이유를 설명하세요.
3. 인덱스가 있는 전체 COUNT에서 버퍼와 입력 행 중 무엇이 줄었나요?
4. bucket42 COUNT와 전체 COUNT의 입력 행 수를 비교하세요.
5. GROUP BY 결과100행과 입력100000행을 구분하고, NULL만 있는 그룹의 SUM을 확인하세요.
6. A의 REPEATABLE READ와 B의 새 조회에서 COUNT가 달라도 각각 정확할 수 있나요?
7. reltuples를 현재 정확한 COUNT로 사용할 수 없는 이유를 관측값으로 설명하세요.
