#!/bin/bash
# usage: clwin_run.sh CORE WORKDIR [BASE=/home/claude/bb/r5/dev] [SEEDS="1 2 3"]
# The click readers on damaged click lines (clwin_sets.py): every configuration (the three-window line code, the cl8g
# kernel loop, their check modes, the scalar twin, direct clicks with and without the exact dedup, other thread counts) must give the answer of the portable route
# (REF: SS_ISA=sse2 SS_CLDIRECT=0 NCPU=1 of REFCORE, default the same binary; use core_c69 to be independent). Prints CLWIN_RUN_PASS or CLWIN_RUN_FAIL.
C=$1; W=$2; BASE=${3:-/home/claude/bb/r5/dev}; SEEDS=${4:-1 2 3}; H=$(cd "$(dirname "$0")" && pwd)
CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py
mkdir -p $W; fail=0
one(){ local c=$1 o=$2; shift 2; mkdir -p $W/$o $W/scr.$o
 env NCPU=1 "$@" SS_STATS=1 DATA_DIR=$D SCRATCH_DIR=$W/scr.$o OUTPUT_DIR=$W/$o $c >$W/$o.log 2>&1 || { echo "FAIL: run $o $* (exit $?): $(grep ERROR $W/$o.log | head -1 | cut -c1-200)"; fail=1; }
 rm -rf $W/scr.$o; }
for seed in $SEEDS; do
 D=$W/set_$seed; [ -d $D ] || python3 $H/clwin_sets.py $BASE $D $seed ${RATE:-0.08} || { echo "FAIL: generator $seed"; fail=1; continue; }
 one ${REFCORE:-$C} ref_$seed SS_ISA=sse2 SS_CLDIRECT=0
 n=0
 for cfg in "SS_CLG=check" "NCPU=3" "NCPU=8 SS_CLG=check SS_CLDIRECT=dedup" "SS_CLG=refcheck" "NCPU=2 SS_CLG=ref SS_CLDIRECT=dedup" "NCPU=3 SS_CLDIRECT=0 SS_CLG=check" "NCPU=3 SS_CLDIRSH=0 SS_TEST_CLEST=1000" "NCPU=5 SS_CLDIRSH=4 SS_CLDIRECT=dedup" "NCPU=3 SS_ISA=sse2" "NCPU=2 SS_CLG=0" "NCPU=2 SS_CLG=0 SS_CLDIRECT=0 SS_CLWIN=check" "SS_CLG=0 SS_CLDIRECT=0 SS_CLWIN=1" "NCPU=3 SS_ISA=avx2 SS_CLWIN=check" "SS_ISA=avx512z SS_CLWIN=check" "NCPU=2 SS_ISA=avx512y SS_CLWIN=check SS_CLDIRECT=dedup" "SS_ISA=avx2 SS_LINES=plain SS_CLWIN=check" "NCPU=5 SS_TEST_MAXNEW=50 SS_CLG=check" "NCPU=4 SS_TEST_MAXNEW=50 SS_CLDIRECT=dedup" "NCPU=2 SS_LANES=0"; do
  n=$((n+1)); one $C o_${seed}_$n $cfg
  r=$(python3 $CMP $W/o_${seed}_$n $W/ref_$seed 2>&1 | tail -1)
  st=$(grep -h 'STATS click line code\|STATS cl8g\|STATS direct clicks' $W/o_${seed}_$n.log | sed 's/STATS //' | tr '\n' ';' | cut -c1-120)
  if [ "$r" = MATCH ]; then echo "ok: seed $seed [$cfg] $st"; else echo "FAIL: seed $seed [$cfg] ($r) $st"; fail=1; fi
  rm -rf $W/o_${seed}_$n
 done
 rm -rf $W/ref_$seed; [ -n "$KEEPSETS" ] || rm -rf $D
done
[ $fail = 0 ] && echo CLWIN_RUN_PASS || echo CLWIN_RUN_FAIL
