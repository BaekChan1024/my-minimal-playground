# 부분 문자열 검색 실습

JDK21, 첫 실행의 Maven/Gradle 다운로드 연결이 필요합니다. Docker·운영 DB는 사용하지 않습니다. 임시 PostgreSQL 18.4를 실행하고 종료 시 닫습니다. 기본 자동 지원은 macOS ARM64이며 다른 플랫폼의 native binary 지원 여부는 의존성을 확인하세요.

```bash
./play-search-01         # 예상 질문, Enter 실행
./play-search-01 verify  # 자동 검증과 계획 수집
python3 search/01-substring-search/summarize.py
```

Mac은 `Search-01.command`를 실행합니다. Python3는 결과 요약에만 필요합니다. 원본 계획은 `search/01-substring-search/build/search-evidence/plans.jsonl`에 남습니다. OS 임시 디렉터리에는 embedded PostgreSQL 라이브러리의 바이너리 캐시가 남을 수 있습니다.

5천/5만 행 합성 데이터, 일곱 검색어, list/count, 기존 B-tree/추가 GIN 세 개/정렬 대응 B-tree 추가를 비교합니다. 각 쿼리 예열 1회 뒤 EXPLAIN ANALYZE 3회를 기록합니다. 병렬/JIT를 꺼 계획 비교를 단순화합니다. 운영 부하 실험이나 애플리케이션 전체 API 응답 시간 측정은 아닙니다.

전체 설명은 [블로그 글](https://blog.baekchan.com/post/블로그-검색에는-어떤-인덱스가-실제로-도움이-될까)에서 읽을 수 있습니다. 고정 태그: `search-substring-01-v1`. 문제는 EXERCISES.md, 해설은 ANSWERS.md를 확인하세요.
