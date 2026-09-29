# 검증 — 2026-09-29

macOS ARM64 / JDK21 / Gradle9.2.1 / Kafka4.1.1 / metadata4.1-IV1. 실제 브로커1·컨트롤러1/RF1. 경계 실험100건 중10건 poll. 후속 파티션2/6조건, 각160건. classic+RangeAssignor, 자동커밋false, max.poll.records10. 업무 후 명시적 commitSync. 데이터·중복·파티션·최종lag 검사. 속도 우열은 통과 조건이 아님.

```text
PASS afterPoll end=100 position=10 committed=0 positionLag=90 committedLag=100 processed=0
PASS afterWork processed=10 committed=10 committedLag=90
```

```csv
case,workers,assigned_workers,p0_records,p1_records,pace_ms,handler_ms,ingress_ms,ingress_records_s,completion_ms,completion_records_s,sample_ms,lag_p0,lag_p1,final_lag
balanced-1,1,1,80,80,0,10,4.83,33112.30,2158.96,74.11,263.33,80,70,0
balanced-2,2,2,80,80,0,10,6.10,26239.19,1101.73,145.23,255.97,70,70,0
balanced-3,3,2,80,80,0,10,3.48,45936.30,1058.80,151.11,260.78,70,70,0
skewed-2,2,2,150,10,0,10,2.84,56429.93,1981.09,80.76,257.41,140,0,0
paced-1,1,1,80,80,12,20,2285.81,70.00,4029.51,39.71,2288.50,30,42,0
paced-2,2,2,80,80,12,20,2265.15,70.64,2364.54,67.67,2267.03,4,4,0
```

```text
PASS scenarios=6 eachRecords=160 exactKeysValuesPartitions=true finalCommittedLag=0 timingAssertions=false
```

자동 verify 종료0. CSV는 단일 실행 실제 관측이며 평균이나 장기 처리능력이 아닙니다. 초기 토픽 생성→그룹 오프셋 초기화에서 메타데이터 전달 지연에 따른 UnknownTopicOrPartitionException을 관측해 해당 오류만 최대15초 재시도하도록 수정 후 전체 재검증했습니다. DB/HTTP·장기부하·운영변경·트랜잭션·장애 중 리밸런스는 미검증.

저장소 밖 Mac Kafka-12.command 안내 실행도 종료0, 모든 데이터 검사 및 최종lag0 확인. 원고 표는 자동 실행 한 번의 CSV이며 안내 실행의 다른 시간값과 섞지 않았습니다.
