#!/bin/bash
# usage: dense_fuzz.sh SEED...  -- the dense route (record loops) against the general route on mutated generated
# sets (fuzz/gen.py): whole-pipeline outputs must match the SS_NODENSE=1 sse2 run at 1 thread, for the default
# at 1 and 3 threads, both AVX-512 mask builders, a forced fallback and the ring (SS_DENSE_AHEAD).
# Prints whether the dense records were in use for events (otherwise the record loops did not run at all).
# The data: a whole generated set (so the ID keys are found; the first 300 lines of each file stay clean for the
# key samples), FZ_NOCDUP=1 (click copies that differ are undefined by the rules and make the slot route depend on
# thread timing), FZ_UNIQ=1 and a new folder per seed (two changed lines must never share a made-up ID).
W=/home/claude/wtA; CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py; D=$W/fz_data_st
mkdir -p $W/fzo
for seed in "$@"; do
 D=$(mktemp -d $W/fzo/sd.XXXXXX) # a new folder every time: files left by another seed would repeat its made-up IDs with other content
 FZ_NOCDUP=1 FZ_UNIQ=1 FZ_CLEAN=300 FZ_LONG=1 FZ_ALL=1 python3 -I $W/fuzz/gen.py /home/claude/bb/mut/base_1$((seed % 6 + 1)) $D $seed >/dev/null || { echo "seed $seed: generator failed"; continue; }
 A=$(mktemp -d $W/fzo/sa.XXXXXX); SS_NODENSE=1 SS_ISA=sse2 NCPU=1 DATA_DIR=$D OUTPUT_DIR=$A SCRATCH_DIR=$A $W/core 2>/dev/null; ra=$?
 L=$(mktemp -d $W/fzo/sl.XXXXXX); SS_DENSE_DUMP=- NCPU=1 DATA_DIR=$D OUTPUT_DIR=$L SCRATCH_DIR=$L $W/core 2>$L/log
 on=$(grep -o "event records [01]" $L/log | head -1); res=""
 for cfg in "NCPU=1" "NCPU=3" "NCPU=2 SS_ISA=avx512z" "NCPU=1 SS_ISA=avx512y" "NCPU=3 SS_TEST_MAXNEW=5" "NCPU=3 SS_DENSE_AHEAD=3" "NCPU=2 SS_DENSE_RECORDS=0" "NCPU=3 SS_LINES=batch" "NCPU=1 SS_LINES=batch SS_ISA=avx512z" "NCPU=3 SS_LINES=ev8" "NCPU=1 SS_LINES=ev8 SS_ISA=avx512z" "NCPU=2 SS_LINES=ev8 SS_ISA=avx512y" "NCPU=3 SS_LINES=2p" "NCPU=2 SS_ISA=avx2"; do
  B=$(mktemp -d $W/fzo/sb.XXXXXX); env $cfg DATA_DIR=$D OUTPUT_DIR=$B SCRATCH_DIR=$B $W/core 2>/dev/null; rb=$?
  res="$res | $cfg: rc $rb $(python3 $CMP $A $B 2>&1 | tail -1)"
 done
 echo "seed $seed ($on, general rc $ra)$res"
done
