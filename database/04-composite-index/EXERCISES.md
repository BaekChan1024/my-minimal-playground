# 먼저 예상하고 실행하기

1. tenant_id=42 AND seq BETWEEN 20000 AND 80000에 어느 키 순서가 적게 읽을지 예상하세요.
2. 두 WHERE 조건의 표기 순서만 바꿨을 때 무엇이 달라질지 예상하고 결과를 비교하세요.
3. 사용자 한 명의 seq DESC LIMIT20과 전체 seq DESC LIMIT20의 정렬 노드를 비교하세요.
4. 최상위 Limit의 Actual Rows와 아래 스캔의 Actual Rows를 각각 읽으세요. 왜 최상위20만으로는 부족한가요?
5. tenant ASC,seq DESC에서 기본 인덱스의 역방향 스캔만으로 충분할까요? Incremental Sort의 입력도 보세요.
6. tenant 조건 없는 마지막11행 조회의 Index Searches와 인덱스 이름을 확인하세요.
7. Index Only Scan이면 언제나 Heap Fetches가0일까요? 실습 준비 조건이 왜 필요한지 설명하세요.

선택 실습은 변경을 보존한 별도 복사본에서 진행하세요.
예를 들어 tenant 종류를 바꾸면 자동 검증의 고정 행 수 조건도 더 이상 맞지 않습니다.
실습 조건을 수정한 결과와 기준 검증 통과를 혼동하지 마세요.
