# 이미지 검증 경계 실습

JDK 21만 필요합니다. 외부 라이브러리·Docker·MinIO 없이 실행합니다.

```bash
./play-image-01          # 예상 질문 → Enter → Java 검증
./play-image-01 verify   # 자동 검증 10개
./play-image-01 serve    # 검증 후 loopback HTTP 서버, 출력 URL 열기
```

Mac은 루트의 `Image-01.command`를 더블클릭하세요. 서버는 Ctrl-C로 종료합니다. 포트 지정은 `./play-image-01 serve 18081`입니다. 기본값은 빈 포트를 자동 선택합니다.

실행 전 [문제](EXERCISES.md)를 읽고, 실행 후 [답](ANSWERS.md)과 [검증](VERIFICATION.md)을 비교하세요.

전체 해설은 [블로그 글](https://blog.baekchan.com/post/이미지-업로드-성공은-어디까지-검증한-걸까)에서 읽을 수 있습니다. 고정 버전은 `image-boundaries-01-v1`입니다.

이 코드는 PNG와 SVG 경계를 설명하는 축소 실험입니다. 운영 업로드 validator로 복사하지 마세요. PNG 시그니처 gate는 의도적으로 전체 디코딩을 하지 않습니다. SVG 예제의 스크립트는 자신의 색상과 문자열만 변경하며 외부 통신을 하지 않습니다. 브라우저 실험은 자동 Java 검증과 별개로 실행해야 합니다.
