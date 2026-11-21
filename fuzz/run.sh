#!/bin/bash
# usage: run.sh SEED...  -- differential test on mutated data, every run into a fresh output folder
# (a reused folder keeps part files of a run with more threads).
#   new AVX2 1 thread vs new SSE2 1 thread: must match.
#   new AVX2 1 thread vs HEAD-baseline SSE2 1 thread: must match when FZ_NONUL=1 (the baseline keeps NUL-dated lines).
#   AVX-512 mask builders (SS_ISA=avx512z / avx512y) 1 thread vs new AVX2 1 thread: must match; their 3-thread runs
#   (one with SS_TEST_TAG0, one with a forced fallback) like the next line.
#   new AVX2 3 threads vs 1 thread: only meaningful with FZ_UNIQ=1. Without it a changed line can share its ID with an unchanged
#   copy, and then the surviving copy depends on thread timing (the v20 baseline behaves the same way).
# Note: this data is a few cut files of dev, so its ID counters lie above the limits estimated from the file
# sizes and the dense route switches itself off: these runs test the general route only. The dense route and
# its record loops are tested by dense_fuzz.sh (whole generated sets) and dense_run.sh / dense_check.py.
# gen.py also makes click copies with other content (undefined by the rules) unless FZ_NOCDUP=1; the general
# route happens to be stable on them, the slot route keeps whichever copy is written last.
W=/home/claude/wtA; CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py; D=$W/fz_data
mkdir -p $W/fzo
for seed in "$@"; do
 python3 -I $W/fuzz/gen.py /home/claude/bb/dev $D $seed || { echo "seed $seed: generator failed"; continue; }
 A=$(mktemp -d $W/fzo/a.XXXXXX); B=$(mktemp -d $W/fzo/b.XXXXXX); C=$(mktemp -d $W/fzo/c.XXXXXX); E=$(mktemp -d $W/fzo/e.XXXXXX)
 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$A $W/core 2>$W/fz_a.log; ra=$?
 SS_ISA=sse2 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$B $W/core 2>$W/fz_b.log; rb=$?
 SS_ISA=sse2 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$C $W/base/core_base 2>$W/fz_c.log; rc=$?
 NCPU=3 DATA_DIR=$D OUTPUT_DIR=$E $W/core 2>$W/fz_e.log; re=$?
 # the same with the test switches: every ingest_ts on the side list (general handler), ID text compared on every probe
 F=$(mktemp -d $W/fzo/f.XXXXXX); G=$(mktemp -d $W/fzo/g.XXXXXX)
 SS_TEST_SIDE=1 SS_TEST_TAG0=1 SS_ISA=sse2 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$F $W/core 2>$W/fz_f.log; rf=$?
 SS_TEST_SIDE=1 SS_TEST_TAG0=1 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$G $W/core 2>$W/fz_g.log; rg=$?
 # the AVX-512 mask builders (they fall back to avx2 on a CPU without AVX-512BW+VL; the log says which path ran)
 Z=$(mktemp -d $W/fzo/z.XXXXXX); Y=$(mktemp -d $W/fzo/y.XXXXXX); Z3=$(mktemp -d $W/fzo/z3.XXXXXX); Y3=$(mktemp -d $W/fzo/y3.XXXXXX)
 SS_ISA=avx512z NCPU=1 DATA_DIR=$D OUTPUT_DIR=$Z $W/core 2>$W/fz_z.log; rz=$?
 SS_ISA=avx512y NCPU=1 DATA_DIR=$D OUTPUT_DIR=$Y $W/core 2>$W/fz_y.log; ry=$?
 SS_ISA=avx512z SS_TEST_TAG0=1 NCPU=3 DATA_DIR=$D OUTPUT_DIR=$Z3 $W/core 2>/dev/null; rz3=$?
 SS_ISA=avx512y SS_TEST_MAXNEW=5 NCPU=3 DATA_DIR=$D OUTPUT_DIR=$Y3 $W/core 2>/dev/null; ry3=$?
 pz=$(grep -h -o 'splitter: .*' $W/fz_z.log | cut -d' ' -f2); py=$(grep -h -o 'splitter: .*' $W/fz_y.log | cut -d' ' -f2)
 vz=$(python3 $CMP $A $Z 2>&1 | tail -1); vy=$(python3 $CMP $A $Y 2>&1 | tail -1); vz3=$(python3 $CMP $A $Z3 2>&1 | tail -1); vy3=$(python3 $CMP $A $Y3 2>&1 | tail -1)
 x=$(python3 $CMP $A $B 2>&1 | tail -1); y=$(python3 $CMP $A $C 2>&1 | tail -1); z=$(python3 $CMP $A $E 2>&1 | tail -1)
 u=$(python3 $CMP $A $F 2>&1 | tail -1); v=$(python3 $CMP $A $G 2>&1 | tail -1)
 echo "seed $seed exit $ra/$rb/$rc/$re/$rf/$rg avx2-vs-sse2 $x | avx2-vs-base-sse2 $y | avx2 1thr-vs-3thr $z | switches sse2 $u avx2 $v | $pz $vz $py $vy (exit $rz/$ry) | 3thr z $vz3 y $vy3 (exit $rz3/$ry3) | $(grep -h -E 'dedup|surviving clicks' $W/fz_a.log | tr '\n' ' ')"
done
