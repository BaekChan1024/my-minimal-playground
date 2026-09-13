# 실행 전에 먼저 예상하기

메모를 직접 작성한 뒤 실행하세요. [해설](ANSWERS.md)은 결과를 확인한 다음 읽습니다.
한 프로그램 안에서 아래 순서로 진행합니다. `read`마다 Consumer가 새로 만들어집니다.

## 1. 레코드의 위치

```text
seed
status study
```

- A, B, C의 offset은 각각 얼마일까?
- 레코드는 3개인데 end offset은 왜 마지막 레코드의 offset과 다를까?
- 아직 커밋이 없다면 lag를 무조건 0이라고 표시해도 될까?

## 2. 읽고 저장하지 않은 위치

```text
read study 2 no-commit
status study
read study 2 commit
```

- 첫 명령 뒤 position과 committed는 각각 무엇일까?
- 두 번째 read에서는 C부터 읽을까, A부터 읽을까? 이유는?
- A와 B를 읽고 나서 커밋하는 숫자를 예상해 보자.

## 3. earliest의 적용 조건

```text
read study 1 commit
read study 1 no-commit
status study
```

- 코드에는 auto.offset.reset=earliest가 있다. 그런데 매번 A부터 읽을까?
- 읽을 것이 없다는 것은 토픽이 비었다는 뜻일까?

## 4. 그룹 바꾸기

```text
read audit 3 commit
status study
status audit
```

- audit은 study가 이미 읽은 레코드를 볼 수 있을까?
- 두 그룹은 무엇을 공유하고 무엇을 따로 저장할까?

## 5. 위치 되돌리기

```text
seek study 0 3
status study
read study 1 no-commit
```

- seek로 0으로 이동하면 study의 저장된 커밋도 0이 될까?
- 다음 read가 다시 A를 읽으려면 무엇이 달라져야 할까?

## 조건을 바꾸는 마지막 실습

`quit` 후 프로그램을 다시 실행합니다. 위 2번의 두 명령에서 COUNT를 모두 2에서 **1**로 바꿉니다.
두 번째 read의 반환 레코드와 committed, 그 다음 read의 시작 위치를 예상한 뒤 비교합니다.

## 숙지 확인

자료를 덮고 세 문장으로 설명하세요.

1. record offset, position, committed offset의 차이는 ___다.
2. 커밋 이후에도 다른 그룹이 기존 레코드를 읽을 수 있는 이유는 ___다.
3. 외부 DB에 저장한 뒤 커밋 전에 중단되면 ___가 일어날 수 있다. 이 실습이 직접 검증한 범위는 ___까지다.

이 설명과 조건 변경 결과를 함께 확인한 뒤 다음 회차로 넘어갑니다.
