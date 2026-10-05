# Database 06 검증

2026-10-05 macOS ARM64, Java21.0.6, PostgreSQL18.4, pgjdbc42.7.10.
자동 검증·저장소 밖 Mac 안내 모드 모두 종료 코드0, 4개 체크포인트 통과.
각 NODE의 hit/read 배분이나 메모리 수치 등은 환경에 따라 달라질 수 있습니다.
부모 버퍼와 자식 버퍼는 중복 합산하지 않습니다. 시간 배수는 검증하지 않습니다.

```text
VISIBILITY customers {"pages" : 7, "allVisible" : 7}
VISIBILITY orders {"pages" : 2128, "allVisible" : 2128}
RESULT narrow root=Nested Loop rows=300 buffers=14
NODE narrow {Node Type=Nested Loop, Actual Rows=300.00, Actual Loops=1, Plan Rows=300, Shared Hit Blocks=13, Shared Read Blocks=1, Temp Read Blocks=0, Temp Written Blocks=0}
NODE narrow {Node Type=Index Only Scan, Relation Name=customers, Index Name=customers_pkey, Parent Relationship=Outer, Actual Rows=3.00, Actual Loops=1, Plan Rows=3, Shared Hit Blocks=3, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
NODE narrow {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Inner, Actual Rows=100.00, Actual Loops=3, Plan Rows=100, Shared Hit Blocks=10, Shared Read Blocks=1, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
PASS narrow-index-probes-and-rows-times-loops
RESULT broad root=Hash Join rows=100000 buffers=394
NODE broad {Node Type=Hash Join, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=13, Shared Read Blocks=381, Temp Read Blocks=0, Temp Written Blocks=0}
NODE broad {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=6, Shared Read Blocks=381, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
NODE broad {Node Type=Hash, Parent Relationship=Inner, Actual Rows=1000.00, Actual Loops=1, Plan Rows=1000, Shared Hit Blocks=7, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Hash Batches=1, Peak Memory Usage=44}
NODE broad {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1000.00, Actual Loops=1, Plan Rows=1000, Shared Hit Blocks=7, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
PASS broad-hash-join-exact-results
RESULT ordered root=Merge Join rows=100000 buffers=392
NODE ordered {Node Type=Merge Join, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=392, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE ordered {Node Type=Index Only Scan, Relation Name=customers, Index Name=customers_pkey, Parent Relationship=Outer, Actual Rows=1000.00, Actual Loops=1, Plan Rows=1000, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
NODE ordered {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Inner, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=387, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
PASS ordered-merge-without-extra-sort
RESULT narrow-without-index root=Hash Join rows=300 buffers=2131
NODE narrow-without-index {Node Type=Hash Join, Actual Rows=300.00, Actual Loops=1, Plan Rows=300, Shared Hit Blocks=2131, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE narrow-without-index {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE narrow-without-index {Node Type=Hash, Parent Relationship=Inner, Actual Rows=3.00, Actual Loops=1, Plan Rows=3, Shared Hit Blocks=3, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Hash Batches=1, Peak Memory Usage=9}
NODE narrow-without-index {Node Type=Index Only Scan, Relation Name=customers, Index Name=customers_pkey, Parent Relationship=Outer, Actual Rows=3.00, Actual Loops=1, Plan Rows=3, Shared Hit Blocks=3, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
PASS same-narrow-result-with-more-input-work-without-index
ALL 4 CHECKPOINTS PASSED. No join method was forced; buffer counts are not latency ratios.
```
