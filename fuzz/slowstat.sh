#!/bin/bash
# usage: slowstat.sh CORE -- share of rows the kernels leave to the line code, on dev (needs an AVX-512 CPU).
# A kernel-side or pairing bug usually cannot give a wrong answer: rows only turn "not ok" and the line code fixes
# them, so the output still matches and only this count shows it. Limits: ev8 (events) 1% (dev: 0.50%),
# cl8 (clicks) 2% (dev: 1.35%). Run for the default kernel (version 4) and for SS_EV8=3.
C=$1; O=$(mktemp -d /tmp/slowstat.XXXXXX); rc=0
for e in "X=1" "SS_EV8=3" "SS_ISA=avx512y"; do
 out=$(env $e NCPU=2 SS_STATS=1 DATA_DIR=/home/claude/bb/dev OUTPUT_DIR=$O SCRATCH_DIR=$O $C 2>&1)
 r=$(echo "$out" | awk -v cfg="$e" '/STATS ev8 loop/{e=1;printf "ev8 [%s] rows %d slow %d (%.2f%%) %s\n",cfg,$5,$NF,100*$NF/$5,($NF*100<$5)?"ok":"FAIL: more than 1% of the event rows left to the line code"}
  /STATS cl8 click loop/{c=1;printf "cl8 [%s] lines %d slow %d (%.2f%%) %s\n",cfg,$6,$NF,100*$NF/$6,($NF*50<$6)?"ok":"FAIL: more than 2% of the click lines left to click_line_v3"}
  END{if(!e)print "FAIL: no STATS ev8 line [" cfg "]";if(!c)print "FAIL: no STATS cl8 line [" cfg "]"}')
 echo "$r"; echo "$r" | grep -q FAIL && rc=1
 find $O -type f -delete
done
find $O -depth -delete
[ $rc = 0 ] && echo SLOWSTAT_OK || echo SLOWSTAT_FAIL
exit $rc
