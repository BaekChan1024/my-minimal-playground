# 실행 후 해설

1. ISR=2일 때 B 성공을 확인합니다.
2. acks=all은 NotEnoughReplicasException, acks=1의 C는 성공합니다. minISR만으로 모든 Producer의 확인 수준을 강제한 실험은 아닙니다.
3. 마지막 목록은 정확히 A,B,C이며 REJECTED 값은 없어야 합니다.
4. ISR=3 복구 후의 정상 리더 중단입니다. ISR=1에서 디스크를 잃는 조건은 실행하지 않았으므로 C가 항상 살아남는다고 일반화할 수 없습니다.
5. RF=3은 ISR=2나 1인 동안에도 유지됩니다. 배치된 복제본과 현재 동기화 참여 집합은 다릅니다.
