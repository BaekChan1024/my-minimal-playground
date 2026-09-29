# 결과 해설

| 실험 | 트랜잭션 안 관측 | reader 종료 뒤 |
|---|---|---|
| jdbc-rc | original→updated | updated |
| jdbc-rr | original→original | updated |
| jpa-rc | 두 번째 find original/추가 SQL0, JDBC·scalar·refresh·clearFind updated | updated |
| jpa-rr | 두 번째 find original/추가 SQL0, JDBC·scalar·refresh·clearFind original | updated |
| readonly-count-list-rc | readOnly=on, count1→ids[1,2]→count2 | count2 |
| readonly-count-list-rr | readOnly=on, count1→ids[1]→count1 | count2 |

RC는 SQL 문장별 가시성, RR은 reader 스냅샷을 유지하는 조건입니다. 반면 JPA find는 이미 관리 중인 같은 ID의 객체를 재사용했습니다. 같은 값을 읽었다는 사실만으로 격리 수준을 판정할 수 없습니다.

refresh는 DB에서 다시 읽지만 DB가 허용하는 스냅샷 안에서 읽습니다. clear 뒤 새 Java 객체를 얻어도 RR 스냅샷은 유지됐습니다. 두 실험 모두 reader 종료 후 실제 DB 값은 updated이므로 writer가 커밋하지 않았기 때문이 아닙니다.

readOnly는 읽기 트랜잭션의 속성이고 스냅샷 고정 옵션이 아닙니다. 실제 DB SHOW transaction_read_only=on 상태에서도 RC count와 목록은 서로 다른 시점을 반영했습니다. 이 실습은 쓰기를 시도해 읽기 전용 enforcement를 검증하지 않습니다.
