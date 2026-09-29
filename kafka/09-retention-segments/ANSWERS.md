# 해설

1. 보호되지 않습니다. 보관 정책과 소비 북마크는 독립입니다.
2. start=8/end=9/committed=0. 남은 레코드를 0으로 재번호하지 않습니다.
3. none은 OffsetOutOfRangeException, earliest는 현재 남은 fresh를 읽습니다.
4. 한 시간 이전 timestamp를 명시한 합성 입력입니다. 시스템 시계나 운영 데이터를 바꾸지 않습니다.
5. 그룹 북마크는 Admin API로 만들고 읽기는 수동 assign/seek로 비교했습니다. subscribe 재가입 실험은 아닙니다.
