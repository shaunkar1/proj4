#!/bin/bash
# usage: run_times.sh CORE SRC_DATA SEED... -- gen_times.py data; the default run at 3 threads is the reference,
# and the per-person sessionizer (SS_TEST_PERSONSESS=1) must give the same tables at 1 and 4 threads, with and
# without the dense route. Every run goes into a fresh output folder, removed after its comparison unless it failed.
W=/home/claude/wtB; CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py; C=$1; SRC=$2; shift 2
mkdir -p $W/fzlo
for seed in "$@"; do
 D=$W/fzt_data; g=$(python3 -I $W/fuzz/gen_times.py $SRC $D $seed) || { echo "seed $seed: generator failed"; continue; }
 R=$(mktemp -d $W/fzlo/tr.XXXXXX); NCPU=3 DATA_DIR=$D OUTPUT_DIR=$R $C 2>$W/fzt_r.log; rr=$?
 bad=
 line="seed $seed ($g) ref rc $rr sessions $(grep -o 'completed sessions: [0-9]*' $W/fzt_r.log | cut -d' ' -f3):"
 for v in "NCPU=1 SS_TEST_PERSONSESS=1" "NCPU=4 SS_TEST_PERSONSESS=1" "NCPU=2 SS_TEST_PERSONSESS=1 SS_NODENSE=1" "NCPU=3 SS_NODENSE=1" "NCPU=2 SS_CELLPASS=1 SS_TEST_TIMEBITS=3" $FZ_EXTRA; do
  O=$(mktemp -d $W/fzlo/to.XXXXXX); env $v DATA_DIR=$D OUTPUT_DIR=$O $C 2>$W/fzt_o.log; rc=$?
  r=$(python3 $CMP $O $R 2>&1 | tail -1)
  if [ $rc = $rr ] && [ "$r" = MATCH ]; then line="$line ok"; rm -r "$O"; else line="$line FAIL[$v rc $rc $r kept $O]"; bad=1; fi
 done
 [ -z "$bad" ] && rm -r "$R" # output folders go as soon as they are compared; a failing seed keeps its reference and the failing runs
 echo "$line"
done
