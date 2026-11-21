#!/usr/bin/env python3
# usage: isa_time.py BIN DATA NCPU RUNS [ISA...]  -- alternates SS_ISA settings; per run the PARSE wall time, the PARSE
# processor time (the program's CPU line, SS_STATS=1) and the whole process's user time; then min and median per setting.
import sys,os,subprocess,resource,time,re,statistics
b,data,ncpu,runs=sys.argv[1],sys.argv[2],sys.argv[3],int(sys.argv[4]); isas=sys.argv[5:] or ['avx2','avx512z','avx512y','sse2']
out='/home/claude/wtA/out_big_%s'%ncpu; os.makedirs(out,exist_ok=True)
res={i:{'pw':[],'pc':[],'user':[],'wall':[]} for i in isas}
for r in range(runs):
    for i in isas:
        env=dict(os.environ,NCPU=ncpu,DATA_DIR=data,OUTPUT_DIR=out,SS_ISA=i,SS_STATS='1')
        r0=resource.getrusage(resource.RUSAGE_CHILDREN); t0=time.time()
        p=subprocess.run([b],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE); wall=time.time()-t0
        r1=resource.getrusage(resource.RUSAGE_CHILDREN); e=p.stderr.decode('latin1')
        used=re.search(r'splitter: (\w+)',e).group(1); pw=float(re.search(r'TIME PARSE ([0-9.e+-]+)',e).group(1)); pc=float(re.search(r'CPU PARSE ([0-9.e+-]+)',e).group(1))
        print('run%d SS_ISA=%s used=%s rc=%d PARSE wall=%.3f cpu=%.3f | process wall=%.2f user=%.2f sys=%.2f'%(r+1,i,used,p.returncode,pw,pc,wall,r1.ru_utime-r0.ru_utime,r1.ru_stime-r0.ru_stime),flush=True)
        if p.returncode==0:
            res[i]['pw'].append(pw);res[i]['pc'].append(pc);res[i]['user'].append(r1.ru_utime-r0.ru_utime);res[i]['wall'].append(wall)
for i in isas:
    d=res[i]
    if d['pw']: print('SUMMARY %-8s PARSE wall min %.3f median %.3f | PARSE cpu min %.3f median %.3f | process user min %.2f median %.2f (%d runs)'%(i,min(d['pw']),statistics.median(d['pw']),min(d['pc']),statistics.median(d['pc']),min(d['user']),statistics.median(d['user']),len(d['pw'])))
