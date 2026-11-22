#!/usr/bin/env python3
# usage: lanes_sets.py BASE OUT KIND [SEED] -- a data set for the lane loops (SS_EVENT_LOOP_EV8L / SS_CLICK_LOOP_CL8L).
# BASE = a generated set (dev); OUT = new folder. Event and click files are rewritten, everything else is linked.
# Every line keeps its meaning or becomes a line that every route drops, so the general route's answer is the
# reference (fuzz/lanes_run.sh). Only ASCII is written (compare.py reads the output as text).
# KIND: merge   all event files in 3 files, all click files in 2 (files of several 2 MB chunks), lines unchanged
#       long    merge + 1.5% of the lines made longer than the scan windows (long quoted product_id / campaign_id that is
#               in no table, long junk lines), so that lane cuts and chunk ends fall into long lines
#       uneven  merge + runs of thousands of empty or very short lines and runs of very long lines (lanes of very
#               different numbers of lines), CR LF line ends, short lines of 60..66 bytes
#       nonl    long, and no file ends with a newline; plus files of 0..9 lines with and without a final newline
#       tiny    event and click files of 0, 1, 2, 7, 8, 9, 15, 16, 17, 63, 64, 65 lines (some without final newline)
import os,sys,random,glob
base,out,kind=sys.argv[1],sys.argv[2],sys.argv[3]
rnd=random.Random(int(sys.argv[4]) if len(sys.argv)>4 else 1)
os.makedirs(out)
for name in os.listdir(base):
    if name in('events','clicks'):continue
    os.symlink(os.path.abspath(os.path.join(base,name)),os.path.join(out,name))
def junk(n):return ''.join(rnd.choice('abcdefghijklmnopqrstuvwxyz0123456789 ;:_-') for _ in range(n))
def longer(line,click):
    f=line.split(',')
    r=rnd.random()
    if r<0.45 and len(f)==(4 if click else 7): # a valid line with a long name that is in no table
        i=2 if click else 5
        f[i]='"'+('c' if click else 'p')+junk(rnd.choice([20,60,70,130,200,900,3000]))+'"'
        return ','.join(f)
    if r<0.6:return junk(rnd.choice([61,62,63,64,65,66,127,128,129,130,500,5000]))          # no comma at all
    if r<0.75:return line+','+junk(rnd.choice([10,64,200]))                                 # one field too many
    if r<0.85:return junk(rnd.choice([58,59,60,61,62,63,64,65]))+','+line                   # third comma around byte 64
    if r<0.93:return line[:rnd.randrange(len(line)+1)]                                      # cut short
    return ''                                                                               # empty
def rewrite(files,sub,nout,click):
    os.makedirs(os.path.join(out,sub))
    head=None;lines=[]
    for fn in sorted(files):
        with open(fn,newline='') as fh:t=fh.read().split('\n')
        if t and t[-1]=='':t.pop()
        head=t[0];lines+=t[1:]
    parts=[lines[i*len(lines)//nout:(i+1)*len(lines)//nout] for i in range(nout)]
    k=0
    def put(ls,final=True,eol='\n'):
        nonlocal k
        with open(os.path.join(out,sub,'part-%05d.csv'%k),'w',newline='') as fh:
            fh.write(head+'\n'+eol.join(ls)+(eol if final and ls else ''))
        k+=1
    if kind=='tiny':
        pos=0
        for n in(0,1,2,7,8,9,15,16,17,63,64,65,1,2,8,9,64,65):
            put(lines[pos:pos+n],final=(k<12));pos+=n
        with open(os.path.join(out,sub,'part-%05d.csv'%k),'w',newline='') as fh:fh.write(head);  # header only, no newline
        k+=1
        h=(len(lines)-pos)//2;put(lines[pos:pos+h]);put(lines[pos+h:],final=False);return   # the rest (the keys need the whole set)
    for pi,ls in enumerate(parts):
        o=[]
        if kind=='merge':o=ls
        elif kind in('long','nonl'):
            for l in ls:
                if rnd.random()<0.015:
                    o.append(longer(l,click))
                    if rnd.random()<0.5:o.append(l)   # the line itself stays as well
                else:o.append(l)
        elif kind=='uneven':
            i=0
            while i<len(ls):
                r=rnd.random() if i>=20000 else 1.0   # the head of each file stays as it is (the keys are found from a sample)
                if r<0.00005:o+=['']*rnd.choice([100,3000,20000])                            # a run of empty lines
                elif r<0.0001:o+=[junk(rnd.randrange(1,8)) for _ in range(rnd.choice([100,5000]))]  # a run of very short lines
                elif r<0.00013:o+=[junk(rnd.choice([300,2000,20000])) for _ in range(rnd.choice([3,40]))] # a run of very long lines
                elif r<0.01:o.append(ls[i][:rnd.choice([60,61,62,63,64,65,66])])
                elif r<0.02:o.append(ls[i]+'\r')
                else:o.append(ls[i])
                i+=1
        put(o,final=(kind!='nonl'))
    if kind=='nonl':
        for n in range(0,10):put(lines[n*10:n*10+n],final=(n%2==0))
rewrite(glob.glob(os.path.join(base,'events','*.csv')),'events',3,False)
rewrite(glob.glob(os.path.join(base,'clicks','*.csv')),'clicks',2,True)
