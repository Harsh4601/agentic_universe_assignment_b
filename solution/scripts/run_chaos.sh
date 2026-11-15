#!/usr/bin/env bash
# Run a matrix of failure-injected trials in parallel, each with random kill -9s, then summarise.
#
#   solution/scripts/run_chaos.sh            # default matrix (about 3-8 minutes real time)
#
# Each trial gets its own mock (own seed and port), database and output file under
# data/trials/<name>/. Exit code is 0 only if every trial passes.
set -u
cd "$(dirname "$0")/../.."
PY=${PY:-.venv/bin/python}
TRIAL=solution/scripts/run_trial.py

#      seed  speed  kills  kill-min kill-max down-max
MATRIX="
       11    600    3      0.5      5        2
       12    600    5      2        25       2
       13    600    4      1        20       10
       14    1200   4      0.5      12       3
       15    600    6      0.3      3        1
       16    1200   3      3        25       10
"

i=0
pids=()
while read -r seed speed kills kmin kmax down; do
  [ -z "${seed:-}" ] && continue
  i=$((i + 1))
  "$PY" "$TRIAL" --seed "$seed" --speed "$speed" --failure-scale 1 --kills "$kills" \
      --kill-min "$kmin" --kill-max "$kmax" --down-max "$down" \
      --provider-port $((8300 + i)) --webhook-port $((8400 + i)) &
  pids+=($!)
done <<< "$MATRIX"

fail=0
for pid in "${pids[@]}"; do
  wait "$pid" || fail=1
done
echo
[ "$fail" -eq 0 ] && echo "ALL TRIALS PASSED" || echo "SOME TRIALS FAILED (see data/trials/*/verify.txt)"
exit "$fail"
