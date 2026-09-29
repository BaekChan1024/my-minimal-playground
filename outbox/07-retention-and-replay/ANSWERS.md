# 해설

1. id1만 삭제, id2와 id3은 유지. sent_at IS NOT NULL 조건 때문에 미전송 id3은 남습니다.
2. 5입니다. 같은 E1은 DUPLICATE입니다.
3. 10입니다. Kafka 이벤트가 두 개여서가 아니라 처리 이력을 잃어 같은 +5가 다시 적용됐습니다.
4. DUPLICATE여서 새 테이블은 0입니다. 기존 이력은 새 결과의 구축 완료가 아닙니다.
5. 별도 rebuild_counter=5, 기존 counter=10 그대로입니다. 같은 업무 테이블에 consumer_name만 바꾸는 것은 안전한 재구축이 아닙니다.
6. 읽기 시작 offset=1, 끝=1이고 auto.offset.reset=none의 seek(0)은 OffsetOutOfRange입니다. Inbox에는 본문이 없어 이벤트 원본을 복구하지 못합니다. 다른 원천/백업은 별도 설계입니다.

cutoff=1월12일이면 최근 완료였던 id2도 삭제되지만 미전송 id3은 여전히 남아야 합니다.
