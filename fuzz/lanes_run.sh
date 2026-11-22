#!/bin/bash
# usage: lanes_run.sh CORE WORKDIR [BASE=/home/claude/bb/dev] [SEED=1] -- the lane loops on the sets of lanes_sets.py.
# KINDS="nonl tiny" picks sets (default: all five); LR_QUICK=1 runs three of the ten configurations.
# Reference per set: the general route (SS_NODENSE=1 SS_ISA=sse2 NCPU=1) of the same binary; every lane configuration
# and the index loops (SS_LANES=0) must give the same answer (compare.py), and the lane runs must say "in lanes" and
# show kernel rows, the same number of rows and of slow rows as the index loops (a lane that processed a line twice
# or walked on past its piece would still give the right answer: only the counts show it). Prints LANES_RUN_PASS or LANES_RUN_FAIL.
C=$1; W=$2; BASE=${3:-/home/claude/bb/dev}; SEED=${4:-1}; H=$(cd "$(dirname "$0")" && pwd)
CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py
mkdir -p $W; fail=0
one(){ # set, out-name, env...
 local d=$1 o=$2; shift 2; local O=$W/$o S=$W/scr.$o; mkdir -p $O $S
 env "$@" SS_STATS=1 DATA_DIR=$d SCRATCH_DIR=$S OUTPUT_DIR=$O $C >$W/$o.log 2>&1 || { echo "FAIL: run $o $* (exit $?)"; fail=1; }
 find $S -type f -delete 2>/dev/null; find $S -depth -type d -delete 2>/dev/null
}
for kind in ${KINDS:-merge long uneven nonl tiny}; do
 D=$W/set_$kind; [ -d $D ] || python3 $H/lanes_sets.py $BASE $D $kind $SEED || { echo "FAIL: generator $kind"; fail=1; continue; }
 one $D ref_$kind SS_NODENSE=1 SS_ISA=sse2 NCPU=1
 one $D ix_$kind NCPU=1 SS_LANES=0; want=$(grep -h 'STATS ev8\|STATS cl8' $W/ix_$kind.log | sed 's/STATS //' | tr '\n' ';') # rows and slow rows of the index loops: the lanes must show the same
 n=0
 for cfg in "NCPU=1" "NCPU=2" "NCPU=5" "NCPU=8" "NCPU=3 SS_ISA=avx512y" "NCPU=2 SS_ISA=avx2 SS_LINES=ev8 SS_CLICKS=cl8" "NCPU=2 SS_LANES=e" "NCPU=2 SS_LANES=c" "NCPU=2 SS_LANES=0" "NCPU=1 SS_LANES=0"; do
  [ -n "$LR_QUICK" ] && [ $n -ge 2 ] && [ $n -ne 5 ] && { n=$((n+1)); continue; } # LR_QUICK=1: configurations 1, 2 and 6 only (bug-injection runs)
  n=$((n+1)); one $D o_${kind}_$n $cfg
  r=$(python3 $CMP $W/o_${kind}_$n $W/ref_$kind 2>&1 | tail -1)
  st=$(grep -h 'STATS ev8\|STATS cl8\|STATS lanes' $W/o_${kind}_$n.log | sed 's/STATS //' | tr '\n' ';')
  if [ "$r" = MATCH ]; then echo "ok: $kind [$cfg] $st"; else echo "FAIL: $kind [$cfg] ($r) $st"; fail=1; fi
  case "$cfg" in *SS_LANES=0*|*SS_ISA=avx2*) ;; *) grep -q "in lanes" $W/o_${kind}_$n.log || { echo "FAIL: $kind [$cfg] did not run in lanes"; fail=1; };; esac
  got=$(grep -h 'STATS ev8\|STATS cl8' $W/o_${kind}_$n.log | sed 's/STATS //' | tr '\n' ';'); [ "$got" = "$want" ] || { echo "FAIL: $kind [$cfg] rows / slow rows differ from the index loops: $got | $want"; fail=1; }
  grep -q "STATS ev8 loop: rows [1-9]" $W/o_${kind}_$n.log || { echo "FAIL: $kind [$cfg] no kernel rows"; fail=1; }
  find $W/o_${kind}_$n -type f -delete; find $W/o_${kind}_$n -depth -type d -delete
 done
done
[ $fail = 0 ] && echo LANES_RUN_PASS || echo LANES_RUN_FAIL
