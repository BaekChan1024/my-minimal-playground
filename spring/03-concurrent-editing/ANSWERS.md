# 결과와 해설

| 조건 | B 결과 | 최종 값 | 관측용 Outbox |
|---|---|---|---|
| 버전 없는 겹친 수정 | committed | B | 사용하지 않음 |
| @Version 겹친 수정 | OptimisticLockException | A:1 | A:1 |
| 오래된 요청, @Version만 | committed | B:2 | A:1,B:2 |
| 오래된 요청, expectedVersion 확인 | EditConflict | A:1 | A:1 |
| 행 잠금만 | 대기 후 committed | B:2 | A:1,B:2 |
| 행 잠금과 expectedVersion 확인 | 대기 후 EditConflict | A:1 | A:1 |

A:1은 제목 A, 엔티티 version1입니다. Outbox 열은 actor:aggregate_version 목록입니다.

- 1번: 이 예제의 UPDATE는 `where id=?`라서 과거에 읽은 상태를 확인하지 않습니다. B로 덮어씁니다.
- 2번: version WHERE 조건으로 A 커밋 뒤 version0 UPDATE가 거절됩니다. 검사 대상은 예외뿐 아니라 최종 제목/버전/Outbox입니다.
- 3번: B의 새 트랜잭션은 version1을 읽었습니다. Hibernate는 version1→2를 정상 갱신합니다. 사용자 초안이 version0에서 시작했다는 정보는 별도 expectedVersion으로 전달하고 비교해야 합니다. 탈착 엔티티 merge의 동작을 일반화한 결과는 아닙니다.
- 4번: 잠금은 서버 트랜잭션의 접근을 조정합니다. B가 대기 후 version1을 읽더라도 B의 요청 본문은 그대로입니다. 비교가 없으면 B로 바뀝니다.
- 5번: 두 잠금 실험 모두 A가 B를 막는 동안 별도 일반 SELECT는 original:0을 읽었습니다. SELECT FOR UPDATE와 모든 조회를 혼동하지 마세요.
- 6번: 숫자만 새 값으로 바꾸면 보호 장치를 통과해 A를 덮어쓸 수 있습니다. 제목 편집은 현재 내용과 내 초안을 비교한 뒤 다시 저장하는 흐름이 필요합니다.

잠금 실험은 처음부터 `find(..., PESSIMISTIC_WRITE)`로 조회합니다. 먼저 불러온 관리 엔티티에 나중에 lock/refresh를 수행하는 흐름이나 특정 운영 서비스 전체를 검증한 것은 아닙니다.
