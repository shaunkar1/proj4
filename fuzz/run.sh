#!/bin/bash
# usage: run.sh SEED...  -- differential test on mutated data, every run into a fresh output folder
# (a reused folder keeps part files of a run with more threads).
#   new AVX2 1 thread vs new SSE2 1 thread: must match.
#   new AVX2 1 thread vs HEAD-baseline SSE2 1 thread: must match when FZ_NONUL=1 (the baseline keeps NUL-dated lines).
#   new AVX2 3 threads vs 1 thread: only meaningful with FZ_UNIQ=1. Without it a changed line can share its ID with an unchanged
#   copy, and then the surviving copy depends on thread timing (the v20 baseline behaves the same way).
W=/home/claude/wtA; CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py; D=$W/fz_data
mkdir -p $W/fzo
for seed in "$@"; do
 python3 -I $W/fuzz/gen.py /home/claude/bb/dev $D $seed || { echo "seed $seed: generator failed"; continue; }
 A=$(mktemp -d $W/fzo/a.XXXXXX); B=$(mktemp -d $W/fzo/b.XXXXXX); C=$(mktemp -d $W/fzo/c.XXXXXX); E=$(mktemp -d $W/fzo/e.XXXXXX)
 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$A $W/core 2>$W/fz_a.log; ra=$?
 SS_ISA=sse2 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$B $W/core 2>$W/fz_b.log; rb=$?
 SS_ISA=sse2 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$C $W/base/core_base 2>$W/fz_c.log; rc=$?
 NCPU=3 DATA_DIR=$D OUTPUT_DIR=$E $W/core 2>$W/fz_e.log; re=$?
 x=$(python3 $CMP $A $B 2>&1 | tail -1); y=$(python3 $CMP $A $C 2>&1 | tail -1); z=$(python3 $CMP $A $E 2>&1 | tail -1)
 echo "seed $seed exit $ra/$rb/$rc/$re avx2-vs-sse2 $x | avx2-vs-base-sse2 $y | avx2 1thr-vs-3thr $z | $(grep -h -E 'dedup|surviving clicks' $W/fz_a.log | tr '\n' ' ')"
done
