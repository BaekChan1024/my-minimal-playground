# 실행 후 해설

- 같은600행의 범위 조회에서 tenant-first 버퍼4, seq-first170이었다. 동일 결과와 인덱스 사용만으로 작업량까지 같지 않다.
- AND 표기 순서만 바꾸면 기준 실행은 같은 인덱스·600행·버퍼4였다. B-tree 키 순서는 CREATE INDEX에 따른다.
- tenant42의 최신20개는 양쪽 모두 역방향 스캔으로 정렬 없이 출력했다. 버퍼는3과9였다.
- 전체 최신20개에서는 tenant-first가 인덱스100000행→Sort→Limit20, seq-first는 역방향20행→Limit20이었다.
- tenant ASC,seq DESC의 기본 인덱스는 Incremental Sort와 스캔1001행을 관측했다. 혼합 키 방향에 맞춘 인덱스는20행, 정렬없음이었다.
- 선두 조건 없이 seq99990~100000을 조회해도 tenant-first 인덱스가 사용됐고 Index Searches101, 실제11행이었다.
  PostgreSQL18 skip scan과 맞는 관측이며 내부101회 각각의 목적을 추적한 결과는 아니다.
- Index Only Scan의 heap 접근 여부에는 가시성도 관여한다. 기준 실습은 all-visible 준비와 실제 Heap Fetches0을 확인한다.
- 버퍼 접근 비율을 응답 시간 개선 배수로 바꿔 말할 수 없다. 부모와 자식의 버퍼를 다시 합치면 중복된다.

이는 자동 실습의 확인 결과이며 사용자 학습 완료를 대신하지 않는다.
