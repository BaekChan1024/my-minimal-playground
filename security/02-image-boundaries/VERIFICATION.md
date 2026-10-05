# 검증 기록 — 2026-10-05

macOS ARM64, JDK 21.0.6에서 자동 10개 PASS 및 저장소 밖 Mac 안내 실행 종료0.

```text
PASS valid-png gate=true decode=true
PASS mime-mismatch gate=false
PASS signature-only gate=true decode=false
PASS empty rejected
PASS over-10MiB rejected
PASS 10MiB boundary gate=true
PASS normal-svg accepted
PASS doctype rejected
PASS wrong-namespace rejected
PASS script-element survives XML acceptance
ALL 10 CHECKS PASSED; Java=21.0.6
브라우저 비교: ./play-image-01 serve 후 출력 URL 열기. 답은 ANSWERS.md에서 확인하세요.
```

실제 loopback HTTP + Codex 내장 브라우저에서 동일 SVG를 비교했습니다.

| 문맥 | 결과 |
|---|---|
| SVG 문서 iframe, nosniff | 초록 EXECUTED |
| SVG 문서 iframe, nosniff + 응답 CSP | 빨강 UNCHANGED |
| img, nosniff | 빨강 UNCHANGED, naturalWidth=360 |

CSP: `sandbox; default-src 'none'; img-src data:; style-src 'unsafe-inline'`.

자동 10개 검증에는 브라우저 결과가 포함되지 않습니다. 운영 API·실제 객체 저장·모든 브라우저·모든 이미지 포맷·파서 자원 고갈·악성코드 검사는 범위 밖입니다. 10MiB 경계 사례는 크기 gate만 확인하며 정상 포맷을 뜻하지 않습니다.
