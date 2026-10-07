# 검색 요청 · 캐시 · 취소 실습

Node.js 22 이상과 npm이 필요합니다. Docker·DB·API 키가 필요 없습니다.
글 전문은 [개발 노트](https://blog.baekchan.com)에 기고 예정이며 현재 발행 대기입니다.
고정 실습 버전: `frontend-query-01-v1`.

```sh
git clone https://github.com/BaekChan1024/my-minimal-playground.git
cd my-minimal-playground
git switch --detach frontend-query-01-v1
./play-frontend-01
```

Mac은 `Frontend-01.command`를 더블클릭합니다. 저장소 위치와 무관하게 실행됩니다.
자동 검증만 하려면 `./play-frontend-01 --verify`를 사용합니다. 최초 실행에는 npm 다운로드가 필요합니다.
직접 실행: 이 폴더에서 `npm ci --ignore-scripts --no-audit --no-fund` 후 `npm test`.

## 실행 전에 예상하기

1. A를 요청한 다음 B를 요청하고 응답은 B→A 순서라면 마지막 결과는?
2. 검색어별 queryKey가 있으면 취소 없이도 B 결과를 유지할까? A 캐시는?
3. signal을 fetch에 전달하면 A 캐시와 서버 작업은 각각 어떻게 될까?
4. queryKey에서 검색어를 빼고 queryFn만 바꾸면 B 요청이 나갈까?
5. 완료된 A→B→A를 30초 안에 오가면 A를 두 번 요청할까?
6. 그때 invalidateQueries를 호출하면 기존 데이터가 먼저 사라질까?

## 결과 비교

콘솔의 experiment 1~6과 마지막 PASS를 읽고 [ANSWERS.md](ANSWERS.md)와 비교합니다.
실제 HTTP 응답 완료를 release 함수로 제어해 우연한 타이밍에 기대지 않습니다.
실패 시 종료 코드가 0이 아니고, 서버·캐시는 종료 과정에서 정리됩니다.
HTTP 서버는 127.0.0.1의 임시 포트에만 열립니다.

관측 대상은 QueryObserver와 Node fetch입니다. React 화면이나 운영 API를 실행하지 않습니다.
`finalView`는 단일 JS 상태 변수, `current`는 observer의 현재 데이터이며 DOM 화면을 뜻하지 않습니다.
실제 UI에서 확인하려면 해당 React 컴포넌트와 브라우저 통합 검증을 추가해야 합니다.
조건 대기의 5초 제한은 검증 실패 감지용이며 성능 측정값이 아닙니다.

더 해볼 문제: [EXERCISES.md](EXERCISES.md), 실행 환경과 한계: [VERIFICATION.md](VERIFICATION.md).
