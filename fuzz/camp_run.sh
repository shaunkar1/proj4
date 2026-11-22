#!/bin/bash
# usage: camp_run.sh CORE WORKDIR [BASE=/home/claude/bb/dev] [SEEDS="1 2"] -- the campaign lookup of the click loops
# (camp_find: the complete table, SS_CAMP=hash the KeyTable, SS_CAMP=check both) on the sets of camp_sets.py.
# Reference per set: the general route with the KeyTable lookup (SS_NODENSE=1 SS_ISA=sse2 SS_CAMP=hash NCPU=1) of the
# same binary, or of REFCORE if that is set (a build from before the table). Every configuration must give the same
# answer; the check runs must report lookups. Prints CAMP_RUN_PASS or CAMP_RUN_FAIL.
C=$1; W=$2; BASE=${3:-/home/claude/bb/dev}; SEEDS=${4:-1 2}; H=$(cd "$(dirname "$0")" && pwd)
CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py
mkdir -p $W; fail=0
one(){ # core, set, out-name, env...
 local c=$1 d=$2 o=$3; shift 3; local O=$W/$o S=$W/scr.$o; mkdir -p $O $S
 env "$@" SS_STATS=1 DATA_DIR=$d SCRATCH_DIR=$S OUTPUT_DIR=$O $c >$W/$o.log 2>&1 || { echo "FAIL: run $o $* (exit $?)"; fail=1; }
 find $S -type f -delete 2>/dev/null; find $S -depth -type d -delete 2>/dev/null
}
for seed in $SEEDS; do
 D=$W/set_$seed; [ -d $D ] || python3 $H/camp_sets.py $BASE $D $seed || { echo "FAIL: generator $seed"; fail=1; continue; }
 one ${REFCORE:-$C} $D ref_$seed SS_NODENSE=1 SS_ISA=sse2 SS_CAMP=hash NCPU=1
 n=0
 for cfg in "NCPU=1" "NCPU=5" "NCPU=2 SS_CAMP=check" "NCPU=2 SS_CAMP=hash" "NCPU=2 SS_LANES=0" "NCPU=2 SS_LANES=0 SS_CLLEAD=2 SS_CAMP=check" "NCPU=2 SS_CL8=1" "NCPU=2 SS_CL8=1 SS_CAMP=check" \
   "NCPU=2 SS_CLICKS=ix" "NCPU=2 SS_CLICKS=ix SS_CAMP=check" "NCPU=2 SS_ISA=avx2 SS_LINES=ev8 SS_CLICKS=cl8" "NCPU=2 SS_ISA=avx2 SS_LINES=ev8 SS_CLICKS=cl8 SS_CAMP=check" "NCPU=2 SS_LINES=plain" \
   "NCPU=2 SS_ISA=sse2" "NCPU=3 SS_NODENSE=1" "NCPU=3 SS_NODENSE=1 SS_CAMP=check" "NCPU=2 SS_DENSE_RECORDS=bucket" "NCPU=2 SS_DENSE_AHEAD=0 SS_CAMP=check"; do
  n=$((n+1)); one $C $D o_${seed}_$n $cfg
  r=$(python3 $CMP $W/o_${seed}_$n $W/ref_$seed 2>&1 | tail -1)
  st=$(grep -h 'click path\|campaign lookups checked\|STATS cl8' $W/o_${seed}_$n.log | sed 's/STATS //' | tr '\n' ';')
  if [ "$r" = MATCH ]; then echo "ok: seed $seed [$cfg] $st"; else echo "FAIL: seed $seed [$cfg] ($r) $st"; fail=1; fi
  case "$cfg" in *SS_CAMP=check*) grep -q "campaign lookups checked against the KeyTable: [1-9]" $W/o_${seed}_$n.log || { echo "FAIL: seed $seed [$cfg] no lookups checked"; fail=1; };; esac
  find $W/o_${seed}_$n -type f -delete; find $W/o_${seed}_$n -depth -type d -delete
 done
done
[ $fail = 0 ] && echo CAMP_RUN_PASS || echo CAMP_RUN_FAIL
