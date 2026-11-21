#!/usr/bin/env python3
# usage: gen_times.py SRC_DATA OUT_DATA SEED -- a copy of a (small) data set whose event and click times
# sit on the edges of the session rules: gaps of exactly 30 min and one ms either side, sessions across
# midnight, clicks exactly seven days before an event and one ms either side, clicks at the moment of an
# event, clicks of one person at the same moment. Every copy of an event or click ID gets the same new
# time, so the result does not depend on which copy survives. With an odd seed the window start moves
# off midnight. For differential tests of the sessionizing code (SS_TEST_PERSONSESS=1 against the default).
import sys,os,random,glob,shutil,json,datetime
src,out,seed=sys.argv[1],sys.argv[2],int(sys.argv[3])
R=random.Random(seed)
if os.path.isdir(out): shutil.rmtree(out)
for d in ('events','clicks','links'): os.makedirs(os.path.join(out,d))
for f in ('products.csv','campaigns.csv'): shutil.copy(os.path.join(src,f),os.path.join(out,f))
for f in glob.glob(os.path.join(src,'links','*.csv')): shutil.copy(f,os.path.join(out,'links',os.path.basename(f)))
man=json.load(open(os.path.join(src,'manifest.json')))
def ms(t): return int(datetime.datetime.strptime(t,'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()*1000)
def iso(m): return datetime.datetime.fromtimestamp(m//1000,datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
ws,we=ms(man['window_start']),ms(man['window_end'])
if seed&1: ws+=R.randrange(1,86400)*1000
man['window_start']=iso(ws); json.dump(man,open(os.path.join(out,'manifest.json'),'w'))
DAY,GAP,WEEK=86400000,1800000,7*86400000
def unq(x): return x[1:-1] if len(x)>=2 and x[:1]=='"' and x[-1:]=='"' else x
def read(d):
    files=[]
    for f in sorted(glob.glob(os.path.join(src,d,'*.csv'))):
        L=open(f,newline='').read().split('\n'); files.append((os.path.basename(f),L))
    return files
ev=read('events'); cl=read('clicks')
# events: first-seen user of each event ID, then a chain of times per user
first={}; order={}
for name,L in ev:
    for l in L[1:]:
        c=l.split(',')
        if len(c)<7: continue
        e=unq(c[0])
        if e not in first: first[e]=unq(c[1]); order.setdefault(first[e],[]).append(e)
newts={}; usertimes={}
for u,ids in order.items():
    if R.random()<0.35: continue            # this user keeps the original times
    t=R.randrange(ws,we); times=[]
    for e in ids:
        newts[e]=t; times.append(t)
        r=R.random()
        if r<0.30: d=R.choice((GAP-1,GAP,GAP+1))
        elif r<0.45: d=R.choice((0,0,1,999,1000))
        elif r<0.60: d=R.randrange(0,GAP)
        elif r<0.70: d=((t//DAY+1)*DAY-t)+R.choice((-1,0,1,-GAP,GAP-1,GAP))   # around the next midnight
        elif r<0.80: d=R.choice((WEEK-1,WEEK,WEEK+1,WEEK-GAP))
        else: d=R.randrange(GAP,3*DAY)
        t+=max(d,0)
        if t>=we: t=R.randrange(ws,we)
    usertimes[u]=times
def fmt(t): return str(t) if (t%1000 or R.random()<0.5) else iso(t)
for name,L in ev:
    o=[L[0]]
    for l in L[1:]:
        c=l.split(',')
        if len(c)>=7 and unq(c[0]) in newts: c[2]=fmt(newts[unq(c[0])]); l=','.join(c)
        o.append(l)
    open(os.path.join(out,'events',name),'w',newline='').write('\n'.join(o))
# clicks: one new time per click ID, tied to an event time of the click's (first-seen) user
cts={}; last={}
for name,L in cl:
    o=[L[0]]
    for l in L[1:]:
        c=l.split(',')
        if len(c)>=4:
            k=unq(c[0]); u=unq(c[1])
            if k not in cts:
                t=None
                if u in usertimes and R.random()<0.8:
                    b=R.choice(usertimes[u]); r=R.random()
                    if r<0.35: t=b-WEEK+R.choice((-1,0,1))
                    elif r<0.60: t=b+R.choice((-1,0,1))
                    elif r<0.80 and u in last: t=last[u]                 # same moment as another click of this user
                    else: t=b-R.randrange(0,WEEK+DAY)
                    if not (ws-WEEK<=t<we): t=None
                cts[k]=t
                if t is not None: last[u]=t
            if cts[k] is not None: c[3]=fmt(cts[k]); l=','.join(c)
        o.append(l)
    open(os.path.join(out,'clicks',name),'w',newline='').write('\n'.join(o))
print('retimed %d events, %d clicks'%(len(newts),sum(1 for v in cts.values() if v is not None)))
