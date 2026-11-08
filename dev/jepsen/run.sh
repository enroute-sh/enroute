#!/bin/sh
# Runs each workload with each fault alone, then with all faults combined,
# and prints one summary line per run. Exits non-zero if any run is not valid
# or did not finish.
#
#   BIN=../../target/release/enroute TIME=60 ./run.sh [extra lein args]
set -u
cd "$(dirname "$0")"
BIN=${BIN:-../../target/release/enroute}
TIME=${TIME:-60}
TOPOLOGY=${TOPOLOGY:-split}
WORKLOADS=${WORKLOADS:-"w1 w1-v0 w2 w3 w4"}
FAULTS=${FAULTS:-"none partition-ledger partition-compute kill-ledger pause-leader pause-compute kill-compute all"}
LAZYFS=${LAZYFS:-/opt/lazyfs/lazyfs}
OUT=${OUT:-summary.txt}
RUNS=$(mktemp)
failed=0
mkdir -p store
ALL="partition-ledger,partition-compute,kill-ledger,pause-leader,pause-compute,kill-compute"
# Member changes with the faults that most often stop them part way.
MEMBERSHIP="replace-ledger,retire-ledger,partition-ledger,kill-ledger,pause-leader"

for w in $WORKLOADS; do
  for f in $FAULTS; do
    case $f in
      none) n="" ;;
      all) n=$ALL ;;
      membership) n=$MEMBERSHIP ;;
      *) n=$f ;;
    esac
    lazy=""
    # LazyFS slows every write, so only runs that kill ledger nodes use it.
    case $n in *kill-ledger*|*kill-leader*|*replace-ledger*) [ -x "$LAZYFS" ] && lazy="--lazyfs $LAZYFS" ;; esac
    echo "== $w $TOPOLOGY ${f}" >&2
    # shellcheck disable=SC2086
    before=$(readlink -f store/latest || true)
    # Jepsen exits 1 for an invalid history, 2 for an unknown one, and 255
    # when the test crashed.
    lein run test --workload "$w" --topology "$TOPOLOGY" --nemesis "$n" \
      --time-limit "$TIME" --concurrency 10 --binary "$BIN" $lazy "$@" \
      > "store/last-run.log" 2>&1
    code=$?
    [ "$code" -eq 0 ] || failed=1
    after=$(readlink -f store/latest || true)
    # A run that crashed has no results, though it may have a store directory.
    if [ -n "$after" ] && [ "$after" != "$before" ] && [ -f "$after/results.edn" ]; then
      echo "$after" >> "$RUNS"
    else
      echo "$(basename "$BIN") $w $TOPOLOGY $f did not finish (exit $code)" | tee -a "$OUT"
      cp store/last-run.log "store/failed-$w-$TOPOLOGY-$f.log"
    fi
  done
done

# One JVM for every summary: each start costs seconds.
[ -s "$RUNS" ] && xargs -d '\n' lein run -m jepsen.enroute.summary < "$RUNS" 2>/dev/null |
  grep -v JAVA_TOOL | sed "s|^|$(basename "$BIN") |" | tee -a "$OUT"
rm -f "$RUNS"
exit "$failed"
