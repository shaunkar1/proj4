#!/usr/bin/env python3
# usage: timeit.py DATA NCPU RUNS BIN...  -- alternates the binaries; per run: wall, user, sys, minor faults,
# max RSS (getrusage of the child) and the program's own stage times; then the minimum of each per binary.
import sys,os,subprocess,resource,time,re,tempfile
data,ncpu,runs=sys.argv[1],sys.argv[2],int(sys.argv[3]); bins=sys.argv[4:]
out='/home/claude/wtA/out_big_%s'%ncpu; os.makedirs(out,exist_ok=True)
best={b:{} for b in bins}
for i in range(runs):
    for b in bins:
        env=dict(os.environ,NCPU=ncpu,DATA_DIR=data,OUTPUT_DIR=out)
        r0=resource.getrusage(resource.RUSAGE_CHILDREN); t0=time.time()
        p=subprocess.run([b],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        wall=time.time()-t0; r1=resource.getrusage(resource.RUSAGE_CHILDREN)
        m={'wall':wall,'user':r1.ru_utime-r0.ru_utime,'sys':r1.ru_stime-r0.ru_stime,'minflt':r1.ru_minflt-r0.ru_minflt}
        for lab,val in re.findall(r'TIME (\w+) ([0-9.e+-]+)',p.stderr.decode('latin1')): m[lab]=float(val)
        print('%s run%d rc=%d '%(b,i+1,p.returncode)+' '.join('%s=%.2f'%(k,v) if k!='minflt' else 'minflt=%d'%v for k,v in m.items() if k in('wall','user','sys','minflt','PARSE','DEDUP','AGGREGATE')),flush=True)
        if p.returncode==0:
            for k,v in m.items(): best[b][k]=min(best[b].get(k,1e30),v)
for b in bins:
    print('MIN %s '%b+' '.join('%s=%.2f'%(k,v) if k!='minflt' else 'minflt=%d'%v for k,v in best[b].items() if k in('wall','user','sys','minflt','PARSE','DEDUP','AGGREGATE')))
