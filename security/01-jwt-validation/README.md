# JWT 테스트 경계 실습

`jwt()`로 인증 정보를 주입하는 테스트와 실제 RS256 토큰을 검증하는 테스트를 같은 Spring Security 필터 체인에서 비교합니다. 전체 해설은 [인증 테스트가 통과하면 JWT 검증도 끝난 걸까?](https://blog.baekchan.com/post/인증-테스트가-통과하면-jwt-검증도-끝난-걸까)에서 읽을 수 있습니다.

## 실행

JDK 21, Git, 최초 다운로드용 인터넷이 필요합니다. Docker·DB·Keycloak 설치는 필요 없습니다. 검증 환경은 macOS ARM64입니다.

```bash
git clone --branch security-jwt-01-v1 https://github.com/BaekChan1024/my-minimal-playground.git jwt-lab
cd jwt-lab
./play-security-01
# 자동 검증
./play-security-01 verify
```

Mac에서는 `Security-01.command`를 더블클릭할 수 있습니다. 예상 질문을 읽고 Enter, 실행 전 종료는 q입니다. 기존 체크아웃에 변경이 있다면 보존하고 별도 폴더에서 고정 버전을 실행하세요.

[예상 문제](EXERCISES.md)를 먼저 읽고 [해설](ANSWERS.md)과 비교하세요. [검증 기록](VERIFICATION.md)에 실제 출력과 범위가 있습니다.

프로그램은 실행마다 메모리에서 RSA 키를 생성하고 loopback 임의 포트에 **공개 키만** 제공하는 JWKS 서버를 띄웁니다. 개인 키와 토큰은 출력·저장하지 않습니다. API 요청은 MockMvc로 처리하므로 실제 API HTTP 서버는 없습니다. JWKS 조회만 로컬 HTTP입니다. 정상 종료 시 서버를 닫습니다. 서비스 계정·운영 토큰·인증 설정을 사용하지 않습니다.

학습용 GET `/write-check`는 권한 규칙만 확인하고 데이터는 쓰지 않습니다. 고정 clock 및 명시적 audience는 재현용 정책이며 운영 인증 설정을 복제한 것이 아닙니다.
