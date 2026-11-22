#!/bin/bash
# usage: stream_check.sh CORE WORKDIR [SETS...]   -- SS_COMMIT=stream against the direct commit (BUCKETS.md stage A).
# SETS: name:data_dir:reference_dir (reference "-": made here with SS_NODENSE=1 SS_ISA=sse2 NCPU=1 of the same binary).
# Default sets: dev, s3 and two mutated sets. SC_BIG=1 adds big20 (three runs). SC_QUICK=1: a third of the matrix.
# For every set:
#  1. slot sums (SS_SLOTSUM=1, stops after the parse stage): for each loop variant with ONE thread the direct and the
#     stream run must leave the same slot array byte for byte (`all` sum); with 3 and 8 threads the same rows in
#     every slot (`rows` sum: counter, k, ingest, category). The user field is not compared there: a row and its copy
#     with the same ingest_ts can carry the user as raw text or as a number, depending on the loop that parsed each,
#     and with several threads either may come first (two direct runs differ in the same way).
#  2. whole runs with SS_COMMIT=stream over SS_BSH 4..20 x 1/3/8 threads, chunk sizes, every loop variant: the answer
#     must MATCH the reference, the log must say "commit stream" and report stream records.
# Prints STREAM_CHECK_PASS or STREAM_CHECK_FAIL.
C=$1; W=$2; shift 2; CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py; B=/home/claude/bb
SETS=${@:-"dev:$B/dev:$B/dev-answer/dev-answer s3:$B/s3:$B/ref_s3/out mut11:$B/mut/mut_11:- mut14:$B/mut/mut_14:-"}
[ -n "$SC_BIG" ] && SETS="$SETS big20:$B/big20:$B/ref20/out"
mkdir -p $W; fail=0; nrun=0
sum(){ # data, env... -> the SLOTSUM line (or the reason there is none)
 local d=$1; shift; local O=$W/sum.$$; mkdir -p $O
 local out; out=$(env "$@" SS_SLOTSUM=1 SS_DENSE_DUMP=- DATA_DIR=$d SCRATCH_DIR=$O OUTPUT_DIR=$O $C 2>&1); local rc=$?
 find $O -type f -delete 2>/dev/null; find $O -depth -type d -delete 2>/dev/null
 local l; l=$(echo "$out" | grep '^SLOTSUM' | tr '\n' ' ')
 [ -n "$l" ] && echo "$l" || echo "NO SLOTSUM (exit $rc)"
}
whole(){ # name, data, ref, env...
 local nm=$1 d=$2 ref=$3; shift 3; local O=$W/o.$$ S=$W/s.$$; mkdir -p $O $S; nrun=$((nrun+1))
 env "$@" SS_STATS=1 DATA_DIR=$d SCRATCH_DIR=$S OUTPUT_DIR=$O $C >$W/last.log 2>&1; local rc=$?
 local r; r=$(python3 $CMP $O $ref 2>&1 | tail -1)
 local st; st=$(grep -h 'STATS stream' $W/last.log | sed 's/STATS stream: //')
 if [ $rc != 0 ] || [ "$r" != MATCH ]; then echo "FAIL: $nm [$*] rc $rc $r"; fail=1; cp $W/last.log $W/fail.$nrun.log
 else case "$*" in *SS_COMMIT=stream*)
   grep -q 'commit stream' $W/last.log && echo "$st" | grep -q 'records [1-9]' || { echo "FAIL: $nm [$*] the stream path did not run ($st)"; fail=1; };; esac
  [ -n "$SC_VERBOSE" ] && echo "ok: $nm [$*] $st"; fi
 find $O $S -type f -delete 2>/dev/null; find $O $S -depth -type d -delete 2>/dev/null
}
for set in $SETS; do
 nm=${set%%:*}; rest=${set#*:}; d=${rest%%:*}; ref=${rest#*:}
 if [ "$ref" = - ]; then ref=$W/ref_$nm; [ -d $ref ] || { mkdir -p $ref $W/rs; SS_NODENSE=1 SS_ISA=sse2 NCPU=1 DATA_DIR=$d SCRATCH_DIR=$W/rs OUTPUT_DIR=$ref $C >/dev/null 2>&1 || { echo "FAIL: reference run $nm"; fail=1; }; }; fi
 big=0; [ $nm = big20 ] && big=1
 # 1. slot sums
 if [ $big = 1 ]; then VAR=("X=1"); THR="1 8"
 else VAR=("X=1" "SS_LANES=0" "SS_LANES=0 SS_EVLEAD=2" "SS_EV8=3" "SS_LINES=plain" "SS_ISA=sse2" "SS_ISA=avx2" "SS_ISA=avx512z" "SS_ISA=avx512y" "SS_DENSE_AHEAD=0" "SS_DENSE_AHEAD=7" "SS_DENSE_USERS=code" "SS_LINES=batch" "SS_TEST_MAXNEW=50" "SS_PUT=row" "SS_SPF=1" "SS_ISA=avx2 SS_LINES=ev8"); THR="1 3 8"; fi
 [ -n "$SC_QUICK" ] && [ $big = 0 ] && VAR=("X=1" "SS_LANES=0 SS_EVLEAD=2" "SS_ISA=sse2" "SS_LINES=plain" "SS_PUT=row")
 for v in "${VAR[@]}"; do for n in $THR; do
  a=$(sum $d $v NCPU=$n); bsh=$((4+(nrun+n)%13)); b=$(sum $d $v NCPU=$n SS_COMMIT=stream SS_BSH=$bsh); nrun=$((nrun+1))
  case "$a" in SLOTSUM*) ;; *) echo "FAIL: $nm [$v NCPU=$n] direct: $a"; fail=1; continue;; esac
  if [ $n = 1 ] && [ "$v" != "SS_TEST_MAXNEW=50" ]; then [ "$a" = "$b" ] || { echo "FAIL: $nm [$v NCPU=1 SS_BSH=$bsh] slot arrays differ: direct $a | stream $b"; fail=1; }
  else [ "${a%% data *}" = "${b%% data *}" ] || { echo "FAIL: $nm [$v NCPU=$n SS_BSH=$bsh] slot rows differ: direct $a | stream $b"; fail=1; }; fi
  [ -n "$SC_VERBOSE" ] && echo "sum: $nm [$v NCPU=$n SS_BSH=$bsh] $b"
 done; done
 # 2. whole runs
 if [ $big = 1 ]; then whole $nm $d $ref NCPU=8 SS_COMMIT=stream; whole $nm $d $ref NCPU=3 SS_COMMIT=stream SS_BSH=16; whole $nm $d $ref NCPU=8 SS_COMMIT=stream SS_BSH=11 SS_BCH=9; continue; fi
 for bsh in 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do for n in 1 3 8; do
  [ -n "$SC_QUICK" ] && [ $(( (bsh+n)%3 )) != 0 ] && continue
  whole $nm $d $ref NCPU=$n SS_COMMIT=stream SS_BSH=$bsh
 done; done
 for v in "SS_BCH=6" "SS_BCH=8 SS_BSH=7" "SS_BCH=16" "SS_BCH=16 SS_BSH=5" "SS_LANES=0" "SS_LANES=0 SS_EVLEAD=2" "SS_LANES=0 SS_EVLEAD=0" "SS_EV8=3" "SS_LINES=plain" "SS_LINES=batch" "SS_ISA=sse2" "SS_ISA=avx2" "SS_ISA=avx512z" "SS_ISA=avx512y" \
   "SS_DENSE_AHEAD=0" "SS_DENSE_AHEAD=7 SS_TEST_MAXNEW=3000" "SS_DENSE_USERS=code" "SS_TEST_MAXNEW=50" "SS_NOHUGE=1" "SS_TEST_LIME=30000 SS_TEST_LIMC=5000 SS_TEST_LIMI=3000" "SS_TEST_LIMI=1" "SS_RESOLVE_RAW=pass" "SS_TEST_STREAM_EST=1" "SS_TEST_STREAM_EST=1 SS_BSH=6 SS_BCH=6" "SS_BKEEP=1" "SS_PUT=row" "SS_PUT=row SS_BSH=6" "SS_SPF=1" "SS_SPF=1 SS_BSH=7" "SS_SPF=0 SS_BSH=5" "SS_ISA=avx2 SS_LINES=ev8"; do
  for n in 1 5; do whole $nm $d $ref NCPU=$n SS_COMMIT=stream $v; done
 done
 echo "done: $nm ($nrun runs so far)"
done
[ $fail = 0 ] && echo STREAM_CHECK_PASS || echo STREAM_CHECK_FAIL
