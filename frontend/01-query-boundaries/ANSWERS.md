# 해설

1. 실험1은 단일 상태에 완료 순서대로 대입하여 B→A. 실험2는 키가 분리돼 current B, cache A. A에는 observer가 없어졌지만 signal을 읽지 않은 queryFn은 완료하고 캐시에 저장됩니다.
2. 실험3은 signal abort와 이전 연결 close를 확인하고 A 작업을 완료시킵니다. A 캐시 없음, fetchStatus idle, current B. query core는 signal getter 접근을 추적하므로 읽기만 해도 취소 상태 처리에 영향을 줍니다. fetch까지 전달하지 않으면 실제 전송 취소 연결은 끊어집니다. getter만 읽는 변형은 이 버전의 소스에 근거한 추가 문제이며 기본6실험의 실행 관측에는 포함하지 않습니다.
3. 결과를 식별하는 검색어·필터·limit 등이 필요합니다. Infinite Query의 pageParam은 동일 검색의 페이지들을 한 캐시에 쌓기 위한 별도 문맥입니다.
4. 실험5는 ACalls 1, idle. 실험6은 명시적 무효화로 새 요청을 보내며 기존 A 데이터 유지, isFetching true, isPending false, 완료 후 A-v2. staleTime은 일반 재조회 판단의 신선도 기준이지 강제 잠금이 아닙니다.
5. fixture는 연결 종료와 별개로 작업을 끝내도록 작성했습니다. 이 반례로 클라이언트 취소가 서버 취소의 충분조건이 아님을 보여 줍니다. 실제 DB나 서버 프레임워크는 측정하지 않았습니다.
6. 실험4는 key ['search']가 동일해서 A 요청 중 queryFn만 바꿔도 B 요청이 발생하지 않고 A 데이터가 남았습니다. 키에 검색어를 넣으면 검색별 캐시·구독·요청 식별이 일치합니다. 수동 refetch만 추가하면 두 검색을 같은 캐시로 취급하는 설계는 남습니다.
