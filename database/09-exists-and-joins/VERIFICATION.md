# Database 09 검증

2026-10-05 macOS ARM64, Java21.0.6, PostgreSQL18.4, pgjdbc42.7.10.
자동 검증·저장소 밖 Mac 안내 모드 모두 종료 코드0, 6개 체크포인트 통과.
각 NODE의 hit/read 배분이나 메모리 수치 등은 환경에 따라 달라질 수 있습니다.
부모 버퍼와 자식 버퍼는 중복 합산하지 않습니다. 시간 배수는 검증하지 않습니다.

```text
VISIBILITY customers {"pages" : 5, "allVisible" : 5}
VISIBILITY orders {"pages" : 2128, "allVisible" : 2128}
RESULT many_count root=Aggregate rows=1 buffers=44
NODE many_count {Node Type=Aggregate, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=44, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Strategy=Plain}
NODE many_count {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Outer, Actual Rows=50000.00, Actual Loops=1, Plan Rows=50377, Shared Hit Blocks=44, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
RESULT many_exists root=Result rows=1 buffers=2
NODE many_exists {Node Type=Result, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=2, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE many_exists {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=InitPlan, Actual Rows=1.00, Actual Loops=1, Plan Rows=50377, Shared Hit Blocks=2, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=0, Subplan Name=InitPlan 1}
VALUE many count=true exists=true qualifying=50000
PASS 1 many matches: count input vs early stop
RESULT absent_index_count root=Aggregate rows=1 buffers=2
NODE absent_index_count {Node Type=Aggregate, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=2, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Strategy=Plain}
NODE absent_index_count {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Outer, Actual Rows=0.00, Actual Loops=1, Plan Rows=100, Shared Hit Blocks=2, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
RESULT absent_index_exists root=Result rows=1 buffers=2
NODE absent_index_exists {Node Type=Result, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=2, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE absent_index_exists {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=InitPlan, Actual Rows=0.00, Actual Loops=1, Plan Rows=100, Shared Hit Blocks=2, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0, Subplan Name=InitPlan 1}
VALUE absent count=false exists=false
PASS 2 absent with index: both empty probes
RESULT absent_scan_count root=Aggregate rows=1 buffers=2128
NODE absent_scan_count {Node Type=Aggregate, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Strategy=Plain}
NODE absent_scan_count {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=Outer, Actual Rows=0.00, Actual Loops=1, Plan Rows=100, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=100000}
RESULT absent_scan_exists root=Result rows=1 buffers=2128
NODE absent_scan_exists {Node Type=Result, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE absent_scan_exists {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=InitPlan, Actual Rows=0.00, Actual Loops=1, Plan Rows=100, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=100000, Subplan Name=InitPlan 1}
PASS 3 absent without index: both reject 100000 rows
RESULT join root=Hash Join rows=100000 buffers=91
NODE join {Node Type=Hash Join, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=91, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Inner}
NODE join {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=86, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
NODE join {Node Type=Hash, Parent Relationship=Inner, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Hash Batches=1, Peak Memory Usage=44}
NODE join {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
RESULT distinct_join root=Aggregate rows=1000 buffers=91
NODE distinct_join {Node Type=Aggregate, Actual Rows=1000.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=91, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Peak Memory Usage=89, Strategy=Hashed}
NODE distinct_join {Node Type=Hash Join, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=91, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Inner}
NODE distinct_join {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=86, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
NODE distinct_join {Node Type=Hash, Parent Relationship=Inner, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Hash Batches=1, Peak Memory Usage=44}
NODE distinct_join {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
RESULT exists_join root=Nested Loop rows=1000 buffers=2026
NODE exists_join {Node Type=Nested Loop, Actual Rows=1000.00, Actual Loops=1, Plan Rows=1000, Shared Hit Blocks=2026, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Semi}
NODE exists_join {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE exists_join {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Inner, Actual Rows=0.99, Actual Loops=1010, Plan Rows=100, Shared Hit Blocks=2021, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
VALUE customer_results join=100000 distinct=1000 exists=1000 customer42_join_rows=100 missing_customers=10
PASS 4 exact customer identities and join multiplicity
RESULT exists_join_no_index root=Hash Join rows=1000 buffers=2133
NODE exists_join_no_index {Node Type=Hash Join, Actual Rows=1000.00, Actual Loops=1, Plan Rows=1000, Shared Hit Blocks=2133, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Inner}
NODE exists_join_no_index {Node Type=Aggregate, Parent Relationship=Outer, Actual Rows=1000.00, Actual Loops=1, Plan Rows=1000, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Peak Memory Usage=89, Strategy=Hashed}
NODE exists_join_no_index {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE exists_join_no_index {Node Type=Hash, Parent Relationship=Inner, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Hash Batches=1, Peak Memory Usage=44}
NODE exists_join_no_index {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
PASS 5 correlated EXISTS can scan all orders in another plan
RESULT aggregate_trap root=Result rows=1 buffers=2128
NODE aggregate_trap {Node Type=Result, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE aggregate_trap {Node Type=Aggregate, Parent Relationship=InitPlan, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Strategy=Plain, Subplan Name=InitPlan 1}
NODE aggregate_trap {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=Outer, Actual Rows=0.00, Actual Loops=1, Plan Rows=100, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=100000}
VALUE aggregate_trap count=0 exists_count=true exists_row=false
PASS 6 aggregate output row is not source-row existence
ALL 6 CHECKPOINTS PASSED
```
