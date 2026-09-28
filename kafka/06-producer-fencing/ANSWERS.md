# 해설

1. 이 실습의 old.commitTransaction은 ProducerFencedException으로 거절됩니다. old의 미완료 트랜잭션 출력은 RC에 0건입니다. send 응답과 트랜잭션 성공은 다릅니다.
2. RC=1, RU=2입니다. RC에는 replacement의 성공 레코드만, RU에는 old의 중단된 레코드도 남습니다. Producer 응답의 offset과 소비한 정체성/순서를 비교합니다.
3. 실측에서는 둘 다 성공합니다. client.id를 공유해도 transactional.id가 다르므로 같은 트랜잭션 소유자를 인계한 것이 아닙니다.
4. 아닙니다. 두 번째 시나리오에서는 동일 key/value의 두 출력이 모두 RC에 보입니다. 첫 시나리오는 이전 트랜잭션 중단과 읽기 격리의 결과입니다.
5. fenced된 Producer 객체는 닫아야 합니다. 업무 소유권을 확인하지 않고 같은 ID로 새 Producer를 반복 생성하면 다른 정상 소유자를 다시 밀어낼 수 있습니다. 이 실습은 재시도 루프를 구현하지 않습니다.
6. 검증하지 않았습니다. 출력 Consumer는 assign/seek만 사용하며 group.id·그룹 오프셋 커밋이 없습니다. DB/HTTP도 실행하지 않았습니다.
