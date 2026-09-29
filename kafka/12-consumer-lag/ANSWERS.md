# 해설

1. end=100/position=10/commit=0/processed=0이면 positionLag=90, committedLag=100입니다. 처리 10건 후 commit=10이면 committedLag=90입니다.
2. classic Consumer Group의 이 실습에서는 2개만 할당받고 1개는 비어 있습니다.
3. skewed-2의 실제 관측은 P0에만 lag가 남았습니다. 한가한 다른 Consumer가 동일 파티션을 자동 분할하지 않습니다.
4. burst의 소비 시작 뒤 유입은 0이고, paced는 처리 중 새 입력이 있습니다. 각 샘플 시점을 구분하세요. 시간값은 환경마다 달라집니다.
5. 스케줄링·전송 시간으로 실측 유입률이 달라집니다. DB 대신 sleep과 메모리 집합을 사용했으므로 외부 부작용 정합성 검증이 아닙니다. 모든 160건 key/value/partition과 중복 없음 및 최종 그룹 lag=0을 검증했습니다.
