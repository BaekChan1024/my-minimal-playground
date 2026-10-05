# 검증 기록

2026-10-05 macOS ARM64에서 자동 검증과 저장소 밖 Mac 안내 실행(Enter) 모두 종료 코드0.
실제 PostgreSQL을 사용하며 모의 실행 계획이 아니다.

```text
Java=21.0.6 PostgreSQL=18.4 JDBC=42.7.10 blockSize=8192
SETTING shared_buffers=32MB
SETTING random_page_cost=4
SETTING seq_page_cost=1
SETTING default_statistics_target=100
SETTING max_parallel_workers_per_gather=0
SETTING jit=off
RESULT range-narrow node=Index Scan estimated=100 actual=100 loops=1 sharedHit=5 sharedRead=0
PASS narrow-range-index
RESULT range-broad node=Seq Scan estimated=90000 actual=90000 loops=1 sharedHit=2041 sharedRead=0
PASS broad-range-sequential
RESULT range-narrow-sequential node=Seq Scan estimated=100 actual=100 loops=1 sharedHit=2041 sharedRead=0
PASS same-result-different-buffer-work
RESULT stats-initial node=Bitmap Heap Scan estimated=100 actual=100 loops=1 sharedHit=100 sharedRead=2
PASS initial-statistics
STATS initial {"n_distinct" : 1000, "bucket_7_frequency" : 0.001}
RESULT stats-stale node=Bitmap Heap Scan estimated=100 actual=90010 loops=1 sharedHit=1998 sharedRead=0
PASS stale-statistics-underestimate
STATS stale {"n_distinct" : 1000, "bucket_7_frequency" : 0.001}
RESULT stats-refreshed node=Seq Scan estimated=90010 actual=90010 loops=1 sharedHit=3344 sharedRead=699
PASS analyze-corrects-estimate-and-plan
STATS refreshed {"n_distinct" : 1000, "bucket_7_frequency" : 0.9001}
ALL 6 CHECKPOINTS PASSED. No timing threshold or universal selectivity threshold asserted.
```

최상위 실행 Plan의 버퍼만 비교한다. Planning 버퍼 및 부모·자식 합계를 더하지 않는다.
안내 실행의 hit/read 배분은 달라질 수 있다. 시간이나 각 버퍼 숫자의 완전 일치는 검증 조건이 아니다.
원본 JSON에는 전체 실행 시간이 나오지만 이 기록은 성능 개선이나 최적 계획을 증명하지 않는다.
통계 목표1000을 지정한 작은 데이터셋의 추정 일치이다. 일반적인 표본 통계의 오차가 없다는 주장이 아니다.
Windows·Linux, 대규모 데이터, 병렬 실행, generic plan, 조인, 운영 지연·처리량은 미검증이다.
