# 서비스 전환 후 데이터 복구 실습

옛 서비스로 돌아갈 때 전환 후의 새 글·수정·삭제·첨부 파일과 다음 쓰기까지 보존되는지 확인합니다. 전체 설명 글은 [개발 블로그](https://blog.baekchan.com) 발행 준비 중입니다.

## 실행

준비물은 Git, JDK 21, 최초 다운로드를 위한 인터넷입니다. Docker와 별도 DB는 필요 없습니다. 검증 환경은 macOS ARM64이며 다른 OS 실행은 미검증입니다.

```bash
git clone --branch recovery-01-v1 https://github.com/BaekChan1024/my-minimal-playground.git recovery-lab
cd recovery-lab
./play-recovery-01
```

Mac에서는 `Recovery-01.command`를 더블클릭해도 됩니다. 질문을 읽고 Enter로 실행하며 `q`는 실행 전 종료입니다. 기존 저장소를 사용한다면 변경 사항을 보존한 뒤 고정 태그를 별도 체크아웃하세요.

```bash
./play-recovery-01 verify
```

매 실행은 임의 포트의 로컬 PostgreSQL과 자체 생성 임시 디렉터리를 사용합니다. 정상 종료 시 정리됩니다. 운영 접속 정보나 경로를 입력받지 않습니다. 강제 종료 시 임시 파일이 남을 수 있습니다.

## 예상 → 실행 → 비교

먼저 [질문](EXERCISES.md)에 답을 예상하고 실행하세요. 네 개의 `PASS`와 마지막 `ALL 4 RECOVERY CHECKPOINTS PASSED`를 [해설](ANSWERS.md)과 비교합니다. 예상과 다르면 [소스](src/main/java/playground/recovery/RecoveryLab.java)의 `run` 순서를 따라가세요.

실습 범위는 실제 PostgreSQL에서의 행·태그 변환, 기본 키 시퀀스, 임시 파일의 존재·SHA-256 검사입니다. `CREATE TABLE AS`로 학습용 복사본을 만들며 **pg_dump/pg_restore 백업 파일, 실제 애플리케이션 전환, MinIO, 동시 쓰기 차단을 구현하지 않습니다.** 파일은 이미지가 아닌 합성 바이트 fixture입니다. 운영 복구 도구로 사용하지 마세요.

버전·실측 로그·미검증 범위는 [검증 기록](VERIFICATION.md)에 있습니다. 고정 태그는 `recovery-01-v1`입니다.
