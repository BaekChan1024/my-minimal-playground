# 검증 — 2026-09-29

macOS ARM64 / JDK 21 / Gradle 9.2.1 / Kafka broker·client 4.1.1 / metadata 4.1-IV1. 안정 메타데이터를 사용한 KafkaClusterTestKit, 분리 컨트롤러 1개와 브로커 3개가 한 JVM에서 실행됩니다.

자동 verify와 저장소 밖 Mac 안내 실행을 완료했습니다. 네 출력과 레코드 A,B,C의 순서를 assertion으로 대조합니다. minISR 거절 ERROR 로그는 의도한 결과입니다. 단순 ERROR 존재를 PASS로 판정하지 않습니다.

```text
PASS RF=3 ISR=2 minISR=2 acks=all accepted=B
PASS RF=3 ISR=1 minISR=2 acks=all rejected=NotEnoughReplicasException
PASS RF=3 ISR=1 minISR=2 acks=1 accepted=C
PASS recoveredISR=3 leaderChanged=true records=A,B,C rejectedAbsent=true
```

정상 broker shutdown/startup만 실행했습니다. 프로세스 kill, 디스크 유실, 네트워크 분할, 컨트롤러 quorum 장애, 다중 호스트, 성능은 미검증입니다. 테스트 기본 unstable metadata에서 발생한 복구 오류는 최종 안정 메타데이터 설정으로 재검증했으며 그 실패 실행을 성공 근거로 사용하지 않습니다.
