# 검증 기록

- 2026-10-07, macOS, Node v22.22.0, @tanstack/query-core 5.101.2. package-lock.json 고정.
- 실제 loopback HTTP 서버 + Node fetch + QueryObserver, 실험6개/명시적 assertion18개 통과.
- 원시 출력: observed-results.jsonl. 실행시간·처리량 벤치마크 아님.
- 서버 수신과 query 상태를 기다린 뒤 응답 순서를 직접 제어합니다. 취소는 signal 이벤트 및 서버 response close도 확인합니다.
- retry false, staleTime 30000, gcTime Infinity. 실험 종료 시 clear. 운영 설정과 다른 gcTime은 실험 중 캐시 수거 간섭을 막기 위한 조건입니다.
- 브라우저 렌더링·React StrictMode·실제 블로그 API·Gateway·DB 취소는 검증 범위 밖입니다.
- 단일 observer가 A에서 B로 이동합니다. A를 쓰는 다른 observer가 남는 경우는 취소 실험에 포함하지 않습니다.

- Outside-repository Mac launcher: 6 experiments / 18 assertions PASS, exit 0.
