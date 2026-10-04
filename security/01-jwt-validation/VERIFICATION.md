# 2026-10-04 검증

macOS ARM64 / JDK 21 / Gradle 9.2.1. Boot 4.0.1 BOM, Spring/Spring Security 7.0.2, Nimbus JOSE JWT 10.4. gradle.lockfile로 실제 해결 버전을 고정했습니다.

```bash
./gradlew :security:01-jwt-validation:verifyLab --console=plain
```

최종 코드에서 종료 0, BUILD SUCCESSFUL. 최초 의존성 고정은 --write-locks로 실행했습니다. 저장소 밖 임시 디렉터리에서 Security-01.command를 절대 경로로 호출하고 Enter를 전달한 안내 실행도 종료 0, 같은 결과였습니다.

```text
SpringSecurity=7.0.2 Spring=7.0.2
PASS mock-writer status=200 decoderCalls=0
PASS missing-token status=401 decoderCalls=0
PASS signed-writer status=200 decoderCalls=1
PASS wrong-signature status=401 decoderCalls=1
PASS wrong-issuer status=401 decoderCalls=1
PASS expired status=401 decoderCalls=1
PASS issuer-time-only otherAudienceAccepted=true
PASS wrong-audience status=401 decoderCalls=1
PASS valid-reader status=403 decoderCalls=1
PASS realm-author status=200 decoderCalls=1
ALL 10 CHECKS PASSED; mock bypass, real validation and authorization boundaries observed.
```

API는 MockMvc, JWKS는 임의 포트의 loopback HTTP입니다. 키/토큰은 메모리에서 생성하고 출력·저장하지 않습니다. 타입·issuer·고정 시각·audience 정책을 명시했습니다. 권한 검사 URL은 데이터를 쓰지 않는 GET입니다. 두 실행에서 본문의 PASS와 decoder 호출 횟수를 대조했습니다.

실제 Keycloak·discovery·TLS·운영 네트워크·키 교체·캐시 장애·모든 claim 부재/경계·Windows/Linux는 미검증입니다. 이 성공은 사용자 숙지나 운영 보안 감사 완료를 뜻하지 않습니다.
