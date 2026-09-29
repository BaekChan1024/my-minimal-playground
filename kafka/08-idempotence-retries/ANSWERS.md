# 해설

1. 실제 retry 지표 증가 후 복구했고 A,B를 읽었습니다.
2. A,B,A. 새로운 send를 업무 key로 제거하지 않습니다.
3. retryObserved와 Future 미완료, 복구 결과, 새 send 결과, ConfigException을 모두 확인합니다.
4. 아닙니다. ISR 부족의 사전 거절을 재현했습니다. 응답 유실과 멱등 off 대조군은 실행하지 않았습니다.
5. ConfigException. 정상 실습은 max.in.flight=5이며 A 성공 이후 B를 보냅니다. 동시 배치의 역전 시험은 아닙니다.
