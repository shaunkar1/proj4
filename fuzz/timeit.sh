#!/bin/bash
# usage: timeit.sh DATA NCPU RUNS BIN...  -- alternates the binaries; prints per run wall/user/sys/minor faults/max RSS and stage times
D=$1; N=$2; R=$3; shift 3
for i in $(seq 1 $R); do for b in "$@"; do
 O=/home/claude/wtA/out_big; mkdir -p $O
 NCPU=$N DATA_DIR=$D OUTPUT_DIR=$O /usr/bin/time -f "%e %U %S %R %M" -o /home/claude/wtA/time.txt $b 2>/home/claude/wtA/time.log; rc=$?
 read wall user sys minflt rss < /home/claude/wtA/time.txt
 st=$(grep TIME /home/claude/wtA/time.log | awk '{printf "%s=%.2f ",$2,$3}')
 echo "$b run$i rc=$rc wall=$wall user=$user sys=$sys minflt=$minflt rssMB=$((rss/1024)) $st"
done; done
