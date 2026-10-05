# 실행 후 해설

- 인덱스 없는 COUNT는 Aggregate 출력1행·값100000, Seq Scan100000행·버퍼2128입니다.
- COUNT(amount)는 NULL10000개를 제외한90000, SUM은4500000000입니다.
- 가시성 준비 후 전체 Index Only Scan은100000행·Heap Fetches0·버퍼91이며, bucket42는1000행·버퍼4입니다.
- GROUP BY는 Hashed Aggregate 결과100그룹·입력100000행·버퍼2128·메모리32kB입니다. 모든 bucket의 COUNT는1000이며,10의 배수 bucket의 SUM은NULL입니다.
- 행이 없는 조건의 COUNT는0, SUM은NULL입니다.
- A의 유지된 스냅샷 COUNT100000, 새 행이 커밋된 뒤 B는100001, A도 다음 트랜잭션에서는100001입니다.
- reltuples는 삽입 전100000·삽입 직후100000·ANALYZE 후100001이었습니다. 통계 추정치와 정확한 스냅샷 집계는 다른 계약입니다.

이는 자동 실습의 관측이며 사용자 숙지 확인을 대신하지 않습니다.
