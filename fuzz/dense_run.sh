#!/bin/bash
# usage: dense_run.sh CORE BASE_DIR WORK_DIR SEED   (BASE_DIR: a generated data set; WORK_DIR: new directory)
# Makes an adversarial copy of BASE_DIR whose forged counters sit around limits at 60% of the real ones, and
# runs dense_check.py on it with those limits and with a few other settings. Prints DENSE_RUN_PASS at the end.
C=$1; B=$2; W=$3; S=$4; H=$(cd "$(dirname "$0")" && pwd); fail=0
mkdir -p "$W/keys" || exit 1
K=$(SS_STATS=1 NCPU=2 DATA_DIR=$B OUTPUT_DIR=$W/keys SCRATCH_DIR=$W/keys $C 2>&1 | awk '/STATS dense keys/{print $4,$5,$6,int($8*0.6),int($9*0.6),int($10*0.6)}')
[ -n "$K" ] || { echo "FAIL: no keys"; exit 1; }
set -- $K; LE=$4; LC=$5; LI=$6
DM_BIN=1 python3 -I $H/dense_mut.py $B $W/data $S $K || exit 1
n=0
for e in "SS_TEST_LIME=$LE SS_TEST_LIMC=$LC SS_TEST_LIMI=$LI" \
         "SS_TEST_LIME=$((LE+1)) SS_TEST_LIMC=$((LC+1)) SS_TEST_LIMI=$((LI+1)) SS_TEST_COMPLEX=3 SS_DENSE_AHEAD=3" \
         "SS_TEST_LIME=$((LE-1)) SS_TEST_LIMC=$((LC-1)) SS_TEST_LIMI=$((LI-1)) SS_TEST_COMPLEX=2 SS_TEST_SIDE=1" \
         "SS_TEST_MAXNEW=50 SS_TEST_COMPLEX=5" "SS_LINES=index"; do
  n=$((n+1)); r=$(python3 $H/dense_check.py $C $W/data $W/chk $e 2>&1 | tail -6)
  case "$r" in *DENSE_PARSE_OK) echo "ok: $e | $(echo "$r" | tail -2 | head -1 | cut -c1-150)";; *) echo "FAIL: $e"; echo "$r"; fail=1;; esac
done
[ $fail = 0 ] && echo DENSE_RUN_PASS || echo DENSE_RUN_FAIL
