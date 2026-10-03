# 예상하고 실행하기

## 실행 전 예상

1. 20,000행 중 90%를 DELETE하면 `count(*)`, `pg_relation_size`, `pg_total_relation_size`가 모두 90% 줄까요?
2. 일반 VACUUM 후 파일 길이가 그대로여도 무엇이 달라질 수 있을까요?
3. 같은 크기의 18,000행을 다시 넣으면 기존 heap 공간을 쓸 수 있을까요? 인덱스도 같은 결과일까요?
4. REPEATABLE READ에서 실제 조회한 트랜잭션을 열어두면 다른 연결의 DELETE·VACUUM에 어떤 영향을 줄까요?
5. `dead_tuple_count=9000`이면 9,000개를 즉시 제거해도 된다는 뜻일까요?
6. FULL을 항상 실행하면 좋을까요? 일반 트랜잭션 안에 VACUUM을 넣을 수 있을까요?

## 직접 실행

`./play-database-02`를 실행하세요. 다음 출력을 비교하고 자신의 예상과 다른 부분을 적으세요.

- `seed`, `after-delete`, `after-vacuum`
- `after-vacuum`, `after-reinsert`의 heap과 indexes
- `snapshot-open-after-vacuum`, `snapshot-closed-after-vacuum`
- `before-full`, `after-full`의 행 수와 파일 크기

같은 측정에서 `dead`가 삭제 행 수와 같지 않다면, 결과를 오류라고 단정하기 전에 측정 전에 실행한 조회들을 확인하세요.

## 조건 변경 (선택, 기본 검증에는 포함하지 않음)

고정 태그에서 별도 작업 브랜치를 만든 뒤 조건을 한 번에 하나씩 바꾸세요.

- 재삽입 행의 payload 길이를 두 배로 만들면 heap 재사용 검증은 유지될까요?
- 오래된 reader를 READ COMMITTED로 바꾸면 두 번째 SELECT의 결과와 VACUUM 뒤 빈 공간이 어떻게 달라질까요?
- DELETE 조건을 마지막 절반 삭제로 바꾸고 `TRUNCATE FALSE`를 제외하면 파일 끝을 줄일 수 있을까요?

기존 검증 조건이 바뀐 실험의 목적과 맞는지도 검토하세요. 원본 복구 전에 수정 사항을 보존하세요.
자동 검증 성공은 사용자 이해 여부를 대신하지 않습니다.
