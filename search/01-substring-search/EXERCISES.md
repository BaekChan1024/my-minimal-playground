# 예상하고 실행하기

1. 희귀와 희귀표식의 결과 ID가 같아도 실행 계획은 달라질까요?
2. GIN을 추가하면 ORDER BY와 LIMIT도 해결될까요?
3. 최신 20개 조회가 빨라지면 count도 빨라질까요?
4. 100%와 a_b를 그대로 ILIKE에 넣으면 리터럴 검색일까요?
5. title과 excerpt를 공백으로 합치면 원래 OR 검색과 같은가요?

./play-search-01을 실행하고 observed-summary.tsv의 참조 실행과 본인의 build/search-evidence/plans.jsonl을 비교하세요. 정확한 시간/계획 일치는 성공 조건이 아닙니다. 같은 조건의 결과와 count 의미가 유지되는지 먼저 확인하세요.
