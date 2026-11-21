#!/usr/bin/env python3
# usage: parse_time.py CORE DATA NCPU RUNS "K=V K=V" ["K=V ..." ...]
# Parse stage only (SS_DENSE_DUMP=- stops after it): alternates the settings, prints min and median of the
# PARSE wall time and of the PARSE processor time (SS_STATS CPU line) per setting.
import os, re, statistics, subprocess, sys
core, data, ncpu, runs = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]); sets = sys.argv[5:]
out = "/home/claude/wtA/dc/w/out"; os.makedirs(out, exist_ok=True)
res = {s: ([], []) for s in sets}
for r in range(runs):
    for s in sets:
        env = dict(os.environ, NCPU=ncpu, DATA_DIR=data, OUTPUT_DIR=out, SCRATCH_DIR=out, SS_STATS="1", SS_DENSE_DUMP="-")
        env.update(dict(a.split("=", 1) for a in s.split()))
        p = subprocess.run([core], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE); e = p.stderr.decode("latin-1")
        w = re.search(r"TIME PARSE ([\d.e+-]+)", e); c = re.search(r"CPU PARSE ([\d.e+-]+)", e)
        if p.returncode != 0 or not w: print("run failed:", s, p.returncode); continue
        res[s][0].append(float(w.group(1)))
        if c: res[s][1].append(float(c.group(1)))
for s in sets:
    w, c = res[s]
    if w: print("%-52s PARSE wall min %.3f median %.3f | cpu min %s (%d runs)" % (s, min(w), statistics.median(w), "%.3f" % min(c) if c else "-", len(w)))
