# 해설

1. 최소 Compaction 지연을 둔 초기에는 업무 레코드 네 건이 모두 읽힙니다.
2. min.compaction.lag.ms를 0으로 바꾸고 Cleaner를 기다리면 A=new(offset2), B=null(offset3)이 남습니다. 재번호를 매기지 않습니다.
3. null은 Map에서 B를 제거합니다. 빈 문자열은 B를 길이0의 값으로 다시 넣습니다.
4. 정리 전후 Map은 A=new로 같고, 마지막 빈 문자열 전송 뒤 B도 존재합니다.
5. roll marker는 세그먼트 롤을 위한 별도 key이며 업무 비교에서 제외합니다. tombstone 1시간 만료, 다중 파티션, 운영 Cleaner 성능은 실행하지 않았습니다.
