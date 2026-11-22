#!/bin/bash
# usage: cl_faults.sh WORKDIR [LIST="1 2 ..."] -- injected faults in the click readers: each one is a one-line change of a
# copy of main.cpp, built (-O2) and run through clwin_run.sh (damaged click lines, new reading with its check mode
# against the old one) and the real dev set. A fault is SEEN when one of them fails. Prints one line per fault.
W=$1; LIST=${2:-1 2 3 4 5 6 7}; H=$(cd "$(dirname "$0")" && pwd); SRC=$H/../main.cpp
CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py
mkdir -p $W
fault(){ case $1 in
 1) F='if((wB.n&mB)|(L>>6))return false;'; T='if((L>>6))return false;'; N='newline inside the user_id part not noticed';;
 2) F='if((errA|errB|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; T='if((errA|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; N='quote error of the user_id ignored';;
 3) F='f.b1=(unsigned)(bB-p)+z1;f.l1=e1-2*z1;'; T='f.b1=(unsigned)(bB-p)+z1;f.l1=e1-z1;'; N='quoted user_id one byte too long';;
 4) F='if((errA|errB|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; T='if((errA|errB|err)!=0||!c)return false;'; N='a fifth field accepted';;
 5) F='pu.k0=h;pu.u=u;pu.up=up;pu.j=(uint32_t)j;pu.k1=(uint32_t)n;'; T='pu.k0=h;pu.u=u;pu.up=up;pu.j=(uint32_t)j;pu.k1=(uint32_t)n-1;'; N='waiting long user one byte short';;
 6) F='if((errA|errB|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; T='if((errB|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; N='quote error of the click_id ignored';;
 7) F='f.next=bC+L+1;'; T='f.next=bC+L+1+(L==63);'; N='next line off by one when the newline is the last byte of window C';;
 *) return 1;; esac; }
for n in $LIST; do
 fault $n || { echo "fault $n: unknown"; continue; }
 python3 - "$SRC" "$W/main_f$n.cpp" "$F" "$T" <<'PY' || { echo "fault $n: the line to change is not in main.cpp (once)"; continue; }
import sys
s=open(sys.argv[1]).read()
if s.count(sys.argv[3])!=1: sys.exit(1)
open(sys.argv[2],'w').write(s.replace(sys.argv[3],sys.argv[4]))
PY
 g++-12 -std=c++20 -O2 -DNDEBUG -march=x86-64 -fopenmp -static -o $W/core_f$n $W/main_f$n.cpp 2>/dev/null || { echo "fault $n: build failed"; continue; }
 r1=$(RATE=0.3 $H/clwin_run.sh $W/core_f$n $W/cw$n /home/claude/bb/r5/dev "1 2" 2>&1 | grep -c "^FAIL")
 O=$W/o$n; mkdir -p $O $W/s$n
 NCPU=2 DATA_DIR=/home/claude/bb/r5/dev OUTPUT_DIR=$O SCRATCH_DIR=$W/s$n $W/core_f$n >/dev/null 2>&1; r2=$(python3 $CMP $O /home/claude/bb/r5/ref_dev/out 2>&1 | tail -1)
 rm -rf $O $W/s$n $W/cw$n $W/core_f$n $W/main_f$n.cpp
 if [ "$r1" != 0 ] || [ "$r2" != MATCH ]; then echo "fault $n SEEN ($N): damaged-line runs failing $r1, real dev $r2"; else echo "fault $n NOT SEEN ($N)"; fi
done
