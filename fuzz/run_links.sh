#!/bin/bash
# usage: run_links.sh NEW_CORE BASE_CORE SEED...  -- both builds on data with mutated links/products/campaigns
# (FZ_KU=<user key> adds dense-ID mutations; extra environment such as SS_TEST_LIMI applies to NEW_CORE only via FZ_NEWENV):
# same exit code, and the same tables when they succeed (new build with 1 and 3 threads vs base with 1).
W=/home/claude/wtB; CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py; D=$W/fzl_data
NEW=$1; BASE=$2; shift 2; mkdir -p $W/fzlo
for seed in "$@"; do
 python3 -I $W/fuzz/gen_links.py /home/claude/bb/dev $D $seed || { echo "seed $seed: generator failed"; continue; }
 A=$(mktemp -d $W/fzlo/a.XXXXXX); B=$(mktemp -d $W/fzlo/b.XXXXXX); C=$(mktemp -d $W/fzlo/c.XXXXXX)
 env $FZ_NEWENV NCPU=1 DATA_DIR=$D OUTPUT_DIR=$A $NEW 2>$W/fzl_a.log; ra=$?
 env $FZ_NEWENV NCPU=3 DATA_DIR=$D OUTPUT_DIR=$B $NEW 2>$W/fzl_b.log; rb=$?
 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$C $BASE 2>$W/fzl_c.log; rc=$?
 # Output folders are removed as soon as they have been compared; a failing seed keeps its three.
 if [ $rc != 0 ]; then echo "seed $seed exit $ra/$rb/$rc $( [ $ra = $rc ] && [ $rb = $rc ] && echo SAME-FAILURE || echo EXIT-DIFFERS ) $(grep -h ERROR $W/fzl_a.log $W/fzl_c.log | tr '\n' ' ')"
  [ $ra = $rc ] && [ $rb = $rc ] && rm -r "$A" "$B" "$C"; continue; fi
 x=$(python3 $CMP $A $C 2>&1 | tail -1); y=$(python3 $CMP $B $C 2>&1 | tail -1)
 echo "seed $seed exit $ra/$rb/$rc new1-vs-base $x | new3-vs-base $y | $(grep -h 'linked IDs' $W/fzl_a.log $W/fzl_c.log | tr '\n' ' ')"
 [ "$x" = MATCH ] && [ "$y" = MATCH ] && rm -r "$A" "$B" "$C"
done
