# 실행 후 해설

- 전체 정렬64kB는 external merge/Disk17872kB/temp read8912·written9379,64MB는 quicksort/Memory21823kB/temp0입니다.
- 두 전체 정렬의 ID 순서는 같고 Seq Scan100000행·공유 버퍼2500회도 같았습니다.
- 64kB의 LIMIT20은 top-N heapsort/Memory34kB/temp0이지만 입력은100000행입니다.
- 정렬 인덱스를 추가하면 Sort 없이 Index Scan20행·공유 버퍼23회, 같은 최종 ID 순서였습니다.
- 정렬 디스크 공간과 누적 임시 I/O는 다른 관측이며, work_mem은 여러 연산·연결의 합계 제한이 아닙니다.

이는 자동 실습의 관측이며 사용자 숙지 확인을 대신하지 않습니다.
