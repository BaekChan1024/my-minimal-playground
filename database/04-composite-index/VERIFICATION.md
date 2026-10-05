# 실제 PostgreSQL 검증

2026-10-05 macOS ARM64에서 자동6개 조건과 저장소 밖 Mac 안내 실행(Enter) 모두 종료코드0.
JDK21.0.6 / PostgreSQL18.4 / JDBC42.7.10. 의존성은 Gradle lockfile에 고정.

```text
Java=21.0.6 PostgreSQL=18.4 JDBC=42.7.10 blockSize=8192
SETTING shared_buffers=32MB
SETTING seq_page_cost=1
SETTING random_page_cost=4
SETTING max_parallel_workers_per_gather=0
SETTING jit=off
VISIBILITY tenant_first {"pages" : 2128, "allVisible" : 2128}
VISIBILITY seq_first {"pages" : 2128, "allVisible" : 2128}
VISIBILITY mixed_order {"pages" : 2128, "allVisible" : 2128}
RESULT range-tenant-first root=Index Only Scan actual=600 rootBuffers=4 sort=false
NODE range-tenant-first Node[type=Index Only Scan, index=tenant_first_idx, direction=Forward, estimated=600, actual=600, loops=1, hit=1, read=3, searches=1, heap=0]
RESULT range-seq-first root=Index Only Scan actual=600 rootBuffers=170 sort=false
NODE range-seq-first Node[type=Index Only Scan, index=seq_first_idx, direction=Forward, estimated=600, actual=600, loops=1, hit=4, read=166, searches=1, heap=0]
PASS key-order-same-result-different-buffer-work
RESULT predicate-text-reordered root=Index Only Scan actual=600 rootBuffers=4 sort=false
NODE predicate-text-reordered Node[type=Index Only Scan, index=tenant_first_idx, direction=Forward, estimated=600, actual=600, loops=1, hit=4, read=0, searches=1, heap=0]
PASS predicate-text-order-is-not-index-key-order
RESULT tenant-latest-tenant-first root=Limit actual=20 rootBuffers=3 sort=false
NODE tenant-latest-tenant-first Node[type=Limit, index=null, direction=null, estimated=20, actual=20, loops=1, hit=2, read=1, searches=0, heap=0]
NODE tenant-latest-tenant-first Node[type=Index Only Scan, index=tenant_first_idx, direction=Backward, estimated=1000, actual=20, loops=1, hit=2, read=1, searches=1, heap=0]
RESULT tenant-latest-seq-first root=Limit actual=20 rootBuffers=9 sort=false
NODE tenant-latest-seq-first Node[type=Limit, index=null, direction=null, estimated=20, actual=20, loops=1, hit=2, read=7, searches=0, heap=0]
NODE tenant-latest-seq-first Node[type=Index Only Scan, index=seq_first_idx, direction=Backward, estimated=1000, actual=20, loops=1, hit=2, read=7, searches=1, heap=0]
PASS fixed-tenant-backward-order-without-sort
RESULT global-latest-tenant-first root=Limit actual=20 rootBuffers=276 sort=true
NODE global-latest-tenant-first Node[type=Limit, index=null, direction=null, estimated=20, actual=20, loops=1, hit=5, read=271, searches=0, heap=0]
NODE global-latest-tenant-first Node[type=Sort, index=null, direction=null, estimated=100000, actual=20, loops=1, hit=5, read=271, searches=0, heap=0]
NODE global-latest-tenant-first Node[type=Index Only Scan, index=tenant_first_idx, direction=Forward, estimated=100000, actual=100000, loops=1, hit=5, read=271, searches=1, heap=0]
RESULT global-latest-seq-first root=Limit actual=20 rootBuffers=3 sort=false
NODE global-latest-seq-first Node[type=Limit, index=null, direction=null, estimated=20, actual=20, loops=1, hit=3, read=0, searches=0, heap=0]
NODE global-latest-seq-first Node[type=Index Only Scan, index=seq_first_idx, direction=Backward, estimated=100000, actual=20, loops=1, hit=3, read=0, searches=1, heap=0]
PASS global-order-favors-seq-leading-index
RESULT missing-leading-key root=Index Only Scan actual=11 rootBuffers=203 sort=false
NODE missing-leading-key Node[type=Index Only Scan, index=tenant_first_idx, direction=Forward, estimated=10, actual=11, loops=1, hit=203, read=0, searches=101, heap=0]
PASS missing-leading-key-still-uses-index-with-multiple-searches
RESULT mixed-default-index root=Limit actual=20 rootBuffers=5 sort=true
NODE mixed-default-index Node[type=Limit, index=null, direction=null, estimated=20, actual=20, loops=1, hit=5, read=0, searches=0, heap=0]
NODE mixed-default-index Node[type=Incremental Sort, index=null, direction=null, estimated=100000, actual=20, loops=1, hit=5, read=0, searches=0, heap=0]
NODE mixed-default-index Node[type=Index Only Scan, index=tenant_first_idx, direction=Forward, estimated=100000, actual=1001, loops=1, hit=5, read=0, searches=1, heap=0]
RESULT mixed-matching-index root=Limit actual=20 rootBuffers=3 sort=false
NODE mixed-matching-index Node[type=Limit, index=null, direction=null, estimated=20, actual=20, loops=1, hit=1, read=2, searches=0, heap=0]
NODE mixed-matching-index Node[type=Index Only Scan, index=mixed_order_idx, direction=Forward, estimated=100000, actual=20, loops=1, hit=1, read=2, searches=1, heap=0]
PASS mixed-direction-matching-index-removes-sort
ALL 6 CHECKPOINTS PASSED. Buffer observations are not latency guarantees; no scan type was forced.
```

실험 초기에는 일부 heap 접근이 섞여 있었다. 최종 코드는 준비의 all-visible 조건을 직접 확인한다.
세 테이블 모두 relpages=relallvisible=2128이며 관측된 인덱스 전용 스캔의 Heap Fetches는0이었다.
플랜을 강제로 선택하지 않았으며 정확한 실행 시간·hit/read 배분은 테스트 조건이 아니다.
준비 조건 실패는 최대3회의 추가 정리 후 중단한다. 운영에서 반복VACUUM을 권장하는 코드가 아니다.
버퍼 값은 최상위 실행 Plan에서만 읽으며 Planning과 자식 노드를 다시 합산하지 않는다.
캐시통제·운영부하·다른버전·인덱스 쓰기비용·NULL·시간동률·Windows/Linux는 미검증이다.
