#!/usr/bin/env python3
# usage: ab_time.py DATA NCPU RUNS BIN...  -- alternates binaries (environment is passed through); per run PARSE wall and
# PARSE processor time (SS_STATS=1 CPU line); then min and median per binary.
import sys,os,subprocess,re,statistics
data,ncpu,runs=sys.argv[1],sys.argv[2],int(sys.argv[3]); bins=sys.argv[4:]
out='/home/claude/wtA/out_big_%s'%ncpu; os.makedirs(out,exist_ok=True)
res={b:{'pw':[],'pc':[],'dw':[]} for b in bins}
for r in range(runs):
    for b in bins:
        env=dict(os.environ,NCPU=ncpu,DATA_DIR=data,OUTPUT_DIR=out,SS_STATS='1')
        p=subprocess.run([b],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE); e=p.stderr.decode('latin1')
        if p.returncode!=0: print('run%d %s rc=%d'%(r+1,b,p.returncode),flush=True); continue
        pw=float(re.search(r'TIME PARSE ([0-9.e+-]+)',e).group(1)); pc=float(re.search(r'CPU PARSE ([0-9.e+-]+)',e).group(1)); dw=float(re.search(r'CPU DEDUP ([0-9.e+-]+)',e).group(1))
        res[b]['pw'].append(pw);res[b]['pc'].append(pc);res[b]['dw'].append(dw)
for b in bins:
    d=res[b]
    if d['pw']: print('SUMMARY %-12s PARSE wall min %.3f median %.3f | PARSE cpu min %.3f median %.3f | DEDUP cpu min %.3f (%d runs)'%(b,min(d['pw']),statistics.median(d['pw']),min(d['pc']),statistics.median(d['pc']),min(d['dw']),len(d['pw'])))
