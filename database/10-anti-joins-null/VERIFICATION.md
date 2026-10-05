# Database 10 검증

2026-10-05 macOS ARM64, Java21.0.6, PostgreSQL18.4, pgjdbc42.7.10.
자동 검증·저장소 밖 Mac 안내 모드 모두 종료 코드0, 7개 체크포인트 통과.
각 NODE의 hit/read 배분이나 메모리 수치 등은 환경에 따라 달라질 수 있습니다.
부모 버퍼와 자식 버퍼는 중복 합산하지 않습니다. 시간 배수는 검증하지 않습니다.

```text
VALUE before_null=[3]
VALUE before_null=[3]
VALUE before_null=[3]
PASS 1 no-null inputs yield customer 3 in all three forms
VALUE null_not_in=[]
VALUE null_not_exists=[3]
VALUE null_left_join=[3]
VALUE null_filtered_not_in=[3]
PASS 2 inner NULL changes NOT IN result; explicit filtering restores this fixture
VALUE truth_matched=f
VALUE truth_unmatched=NULL
VALUE truth_outer_null_nonempty=NULL
VALUE truth_outer_null_not_exists=t
VALUE truth_outer_null_empty=t
PASS 3 outer NULL and empty subquery semantics
VALUE wrong_nullable_marker=[1, 3]
VALUE safe_primary_key_marker=[3]
PASS 4 nullable note cannot distinguish unmatched rows
VISIBILITY customers {"pages" : 5, "allVisible" : 5}
VISIBILITY orders {"pages" : 2128, "allVisible" : 2128}
VALUE large_not_in=[]
VALUE large_absent=[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
VALUE large_absent=[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
VALUE large_absent=[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
RESULT no_index_not_exists root=Hash Join rows=10 buffers=2133
NODE no_index_not_exists {Node Type=Hash Join, Actual Rows=10.00, Actual Loops=1, Plan Rows=10, Shared Hit Blocks=2133, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Anti}
NODE no_index_not_exists {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE no_index_not_exists {Node Type=Hash, Parent Relationship=Inner, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100001, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Hash Batches=1, Peak Memory Usage=4540}
NODE no_index_not_exists {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=Outer, Actual Rows=100001.00, Actual Loops=1, Plan Rows=100001, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
RESULT no_index_left_join root=Hash Join rows=10 buffers=2133
NODE no_index_left_join {Node Type=Hash Join, Actual Rows=10.00, Actual Loops=1, Plan Rows=10, Shared Hit Blocks=2133, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Anti}
NODE no_index_left_join {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE no_index_left_join {Node Type=Hash, Parent Relationship=Inner, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100001, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Hash Batches=1, Peak Memory Usage=4540}
NODE no_index_left_join {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=Outer, Actual Rows=100001.00, Actual Loops=1, Plan Rows=100001, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
RESULT no_index_not_in root=Seq Scan rows=0 buffers=2133
NODE no_index_not_in {Node Type=Seq Scan, Relation Name=customers, Actual Rows=0.00, Actual Loops=1, Plan Rows=505, Shared Hit Blocks=2133, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=1010, Filter=(NOT (ANY (id = (hashed SubPlan 1).col1)))}
NODE no_index_not_in {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=SubPlan, Actual Rows=100001.00, Actual Loops=1, Plan Rows=100001, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Subplan Name=SubPlan 1}
RESULT no_index_filtered root=Seq Scan rows=10 buffers=2133
NODE no_index_filtered {Node Type=Seq Scan, Relation Name=customers, Actual Rows=10.00, Actual Loops=1, Plan Rows=505, Shared Hit Blocks=2133, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=1000, Filter=(NOT (ANY (id = (hashed SubPlan 1).col1)))}
NODE no_index_filtered {Node Type=Seq Scan, Relation Name=orders, Parent Relationship=SubPlan, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100001, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=1, Subplan Name=SubPlan 1, Filter=(customer_id IS NOT NULL)}
PASS 5 large fixture: compare semantics before comparing plans
VALUE indexed_absent=[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
VALUE indexed_absent=[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
VALUE indexed_absent=[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
RESULT index_not_exists root=Nested Loop rows=10 buffers=2026
NODE index_not_exists {Node Type=Nested Loop, Actual Rows=10.00, Actual Loops=1, Plan Rows=10, Shared Hit Blocks=2026, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Anti}
NODE index_not_exists {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE index_not_exists {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Inner, Actual Rows=0.99, Actual Loops=1010, Plan Rows=100, Shared Hit Blocks=2021, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
RESULT index_left_join root=Nested Loop rows=10 buffers=2026
NODE index_left_join {Node Type=Nested Loop, Actual Rows=10.00, Actual Loops=1, Plan Rows=10, Shared Hit Blocks=2026, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Anti}
NODE index_left_join {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE index_left_join {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Inner, Actual Rows=0.99, Actual Loops=1010, Plan Rows=100, Shared Hit Blocks=2021, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
RESULT index_not_in root=Seq Scan rows=0 buffers=91
NODE index_not_in {Node Type=Seq Scan, Relation Name=customers, Actual Rows=0.00, Actual Loops=1, Plan Rows=505, Shared Hit Blocks=91, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=1010, Filter=(NOT (ANY (id = (hashed SubPlan 1).col1)))}
NODE index_not_in {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=SubPlan, Actual Rows=100001.00, Actual Loops=1, Plan Rows=100001, Shared Hit Blocks=86, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0, Subplan Name=SubPlan 1}
RESULT index_filtered root=Seq Scan rows=10 buffers=91
NODE index_filtered {Node Type=Seq Scan, Relation Name=customers, Actual Rows=10.00, Actual Loops=1, Plan Rows=505, Shared Hit Blocks=91, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=1000, Filter=(NOT (ANY (id = (hashed SubPlan 1).col1)))}
NODE index_filtered {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=SubPlan, Actual Rows=100000.00, Actual Loops=1, Plan Rows=99998, Shared Hit Blocks=86, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0, Subplan Name=SubPlan 1}
PASS 6 index preserves answers while access paths may change
VISIBILITY orders {"pages" : 2128, "allVisible" : 2128}
VALUE not_null_absent=[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
VALUE not_null_absent=[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
VALUE not_null_absent=[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
RESULT not_null_not_in root=Seq Scan rows=10 buffers=115
NODE not_null_not_in {Node Type=Seq Scan, Relation Name=customers, Actual Rows=10.00, Actual Loops=1, Plan Rows=505, Shared Hit Blocks=115, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Rows Removed by Filter=1000, Filter=(NOT (ANY (id = (hashed SubPlan 1).col1)))}
NODE not_null_not_in {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=SubPlan, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=110, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0, Subplan Name=SubPlan 1}
RESULT not_null_not_exists root=Nested Loop rows=10 buffers=2026
NODE not_null_not_exists {Node Type=Nested Loop, Actual Rows=10.00, Actual Loops=1, Plan Rows=10, Shared Hit Blocks=2026, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Anti}
NODE not_null_not_exists {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE not_null_not_exists {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Inner, Actual Rows=0.99, Actual Loops=1010, Plan Rows=100, Shared Hit Blocks=2021, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
RESULT not_null_left_join root=Nested Loop rows=10 buffers=2026
NODE not_null_left_join {Node Type=Nested Loop, Actual Rows=10.00, Actual Loops=1, Plan Rows=10, Shared Hit Blocks=2026, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Join Type=Anti}
NODE not_null_left_join {Node Type=Seq Scan, Relation Name=customers, Parent Relationship=Outer, Actual Rows=1010.00, Actual Loops=1, Plan Rows=1010, Shared Hit Blocks=5, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
NODE not_null_left_join {Node Type=Index Only Scan, Relation Name=orders, Index Name=orders_customer_idx, Parent Relationship=Inner, Actual Rows=0.99, Actual Loops=1010, Plan Rows=100, Shared Hit Blocks=2021, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
PASS 7 non-null keys restore result equivalence; observe rather than assume plan rewrite
ALL 7 CHECKPOINTS PASSED
```
