#!/usr/bin/env bash
"$(cd "$(dirname "$0")" && pwd)/play-frontend-01" "$@"
result=$?
if [[ -t 0 ]]; then read -r -p 'Enter를 누르면 닫습니다. ' answer; fi
exit "$result"
