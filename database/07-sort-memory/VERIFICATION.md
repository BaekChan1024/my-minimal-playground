# Database 07 검증

2026-10-05 macOS ARM64, Java21.0.6, PostgreSQL18.4, pgjdbc42.7.10.
자동 검증·저장소 밖 Mac 안내 모드 모두 종료 코드0, 4개 체크포인트 통과.
각 NODE의 hit/read 배분이나 메모리 수치 등은 환경에 따라 달라질 수 있습니다.
부모 버퍼와 자식 버퍼는 중복 합산하지 않습니다. 시간 배수는 검증하지 않습니다.

```text
RESULT full-64kB root=Sort rows=100000 buffers=2500
NODE full-64kB {Node Type=Sort, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2500, Shared Read Blocks=0, Temp Read Blocks=8912, Temp Written Blocks=9379, Sort Method=external merge, Sort Space Type=Disk, Sort Space Used=17872}
NODE full-64kB {Node Type=Seq Scan, Relation Name=sort_data, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2500, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
PASS full-sort-spills-with-small-work-mem
RESULT full-64MB root=Sort rows=100000 buffers=2500
NODE full-64MB {Node Type=Sort, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2500, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Sort Method=quicksort, Sort Space Type=Memory, Sort Space Used=21823}
NODE full-64MB {Node Type=Seq Scan, Relation Name=sort_data, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2500, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
PASS larger-memory-same-result-without-temp-write
RESULT top20-64kB root=Limit rows=20 buffers=2500
NODE top20-64kB {Node Type=Limit, Actual Rows=20.00, Actual Loops=1, Plan Rows=20, Shared Hit Blocks=2500, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE top20-64kB {Node Type=Sort, Parent Relationship=Outer, Actual Rows=20.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2500, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Sort Method=top-N heapsort, Sort Space Type=Memory, Sort Space Used=34}
NODE top20-64kB {Node Type=Seq Scan, Relation Name=sort_data, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2500, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
PASS top-n-keeps-small-state-but-reads-all-input
RESULT top20-index root=Limit rows=20 buffers=23
NODE top20-index {Node Type=Limit, Actual Rows=20.00, Actual Loops=1, Plan Rows=20, Shared Hit Blocks=20, Shared Read Blocks=3, Temp Read Blocks=0, Temp Written Blocks=0}
NODE top20-index {Node Type=Index Scan, Relation Name=sort_data, Index Name=sort_data_order_idx, Parent Relationship=Outer, Actual Rows=20.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=20, Shared Read Blocks=3, Temp Read Blocks=0, Temp Written Blocks=0}
PASS matching-order-index-avoids-full-input-sort
ALL 4 CHECKPOINTS PASSED. work_mem is per operation, not a total server cap; temp blocks are not physical disk throughput.
```
