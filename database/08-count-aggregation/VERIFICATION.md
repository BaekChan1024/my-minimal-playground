# Database 08 검증

2026-10-05 macOS ARM64, Java21.0.6, PostgreSQL18.4, pgjdbc42.7.10.
자동 검증·저장소 밖 Mac 안내 모드 모두 종료 코드0, 6개 체크포인트 통과.
각 NODE의 hit/read 배분이나 메모리 수치 등은 환경에 따라 달라질 수 있습니다.
부모 버퍼와 자식 버퍼는 중복 합산하지 않습니다. 시간 배수는 검증하지 않습니다.

```text
RESULT count-without-index root=Aggregate rows=1 buffers=2128
NODE count-without-index {Node Type=Aggregate, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Strategy=Plain}
NODE count-without-index {Node Type=Seq Scan, Relation Name=metrics, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
VALUE counts all=100000 nonnull=90000 sum=4500000000
PASS one-output-row-is-not-one-input-row-and-null-counts-differ
VISIBILITY metrics {"pages" : 2128, "allVisible" : 2128}
RESULT count-with-index root=Aggregate rows=1 buffers=91
NODE count-with-index {Node Type=Aggregate, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=1, Shared Read Blocks=90, Temp Read Blocks=0, Temp Written Blocks=0, Strategy=Plain}
NODE count-with-index {Node Type=Index Only Scan, Relation Name=metrics, Index Name=metrics_bucket_idx, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=1, Shared Read Blocks=90, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
PASS index-only-reduces-buffer-work-not-counted-cardinality
RESULT count-bucket42 root=Aggregate rows=1 buffers=4
NODE count-bucket42 {Node Type=Aggregate, Actual Rows=1.00, Actual Loops=1, Plan Rows=1, Shared Hit Blocks=4, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Strategy=Plain}
NODE count-bucket42 {Node Type=Index Only Scan, Relation Name=metrics, Index Name=metrics_bucket_idx, Parent Relationship=Outer, Actual Rows=1000.00, Actual Loops=1, Plan Rows=970, Shared Hit Blocks=4, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Heap Fetches=0}
PASS filtered-count-narrows-input
RESULT grouped root=Aggregate rows=100 buffers=2128
NODE grouped {Node Type=Aggregate, Actual Rows=100.00, Actual Loops=1, Plan Rows=100, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0, Peak Memory Usage=32, Strategy=Hashed}
NODE grouped {Node Type=Seq Scan, Relation Name=metrics, Parent Relationship=Outer, Actual Rows=100000.00, Actual Loops=1, Plan Rows=100000, Shared Hit Blocks=2128, Shared Read Blocks=0, Temp Read Blocks=0, Temp Written Blocks=0}
PASS group-output-cardinality-and-null-sums
VALUE empty count=0 sum=null
PASS empty-count-zero-sum-null
VALUE snapshots held=100000 fresh=100001 nextTransaction=100001
VALUE estimates before=100000 afterInsert=100000 afterAnalyze=100001
PASS exact-count-is-snapshot-dependent-catalog-is-estimate
ALL 6 CHECKPOINTS PASSED. Exact counts, estimates and cached business counters are different contracts.
```
