# 결과 비교

- mock-writer는 200/decoder 0회이며 JWKS 요청도 0회입니다. 이미 인증된 주체와 권한을 제공했으므로 실제 토큰 검증을 증명하지 않습니다.
- signed-writer는 200/decoder 1회이며 실제 공개 JWKS 조회가 발생합니다. wrong-signature는 같은 kid여도 다른 개인 키로 서명했으므로 401입니다.
- issuer-time-only는 서명·시간·issuer·typ은 검사하지만 audience 정책이 없어서 다른 서비스용 aud가 통과합니다. audience validator를 추가한 API 경로에서는 같은 토큰이 401입니다. 이는 실습에서 만든 두 정책의 차이이며 운영 취약성 발견이 아닙니다.
- 만료·issuer·audience 오류는 이 필터 설정에서 401입니다. valid-reader는 decoder 통과 뒤 필요한 권한이 없어서 403입니다. 상태 코드만으로 모든 401 원인을 구분할 수는 없습니다.
- realm-author는 실습의 커스텀 converter가 realm_access.roles를 ROLE_로 변환하므로 200입니다. 기본 scope 매핑은 SCOPE_를 사용합니다. mock-writer처럼 authority를 직접 넣으면 converter의 정확성도 검사하지 않습니다.

이 실습은 실제 암호 검증을 실행하지만 API는 MockMvc입니다. Keycloak 로그인·discovery·TLS·키 교체·네트워크 장애·운영 audience 정책은 검증하지 않습니다. 모든 부정 입력/필수 claim 부재/시간 경계/알고리즘 조합을 포괄하는 보안 감사도 아닙니다. 자동 검증 통과와 사용자의 숙지는 별개입니다.
