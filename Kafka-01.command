#!/usr/bin/env bash
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
"$project_dir/play" "$@"
result=$?
if [[ $result -eq 0 ]]; then
    printf '\n실행이 종료되었습니다.\n'
else
    printf '\n실행하지 못했습니다. 위 오류 메시지를 확인하세요. (종료 코드 %s)\n' "$result"
fi
if [[ -t 0 ]]; then
    printf 'Enter를 누르면 이 실행 도구가 끝납니다. '
    IFS= read -r ignored
fi
exit "$result"
