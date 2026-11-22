#!/usr/bin/env python3
# usage: camp_sets.py BASE OUT [SEED] -- a data set for the campaign lookup of the click loops (camp_find).
# BASE = a generated set (dev); OUT = new folder. campaigns.csv gets extra campaigns whose IDs have 1 to 14 bytes
# (the table of camp_find holds IDs of up to 7 bytes, longer ones go through the KeyTable), and about a fifth of
# the click lines get another campaign_id: one of the extra campaigns, or a text that is no campaign (a known ID
# with a byte changed, cut, lengthened, in other case, with spaces, with leading zeros; texts of 7 / 8 / 11 / 12
# bytes; nothing at all), quoted or not; all copies of a click get the same text (copies that differ are undefined
# data: the routes may keep different ones). The general route's answer is the reference (fuzz/camp_run.sh).
import os,sys,random
base,out=sys.argv[1],sys.argv[2]
rnd=random.Random(int(sys.argv[3]) if len(sys.argv)>3 else 1)
os.makedirs(out);os.makedirs(os.path.join(out,'clicks'))
for name in os.listdir(base):
    if name in('clicks','campaigns.csv'):continue
    os.symlink(os.path.abspath(os.path.join(base,name)),os.path.join(out,name))
rows=open(os.path.join(base,'campaigns.csv')).read().split('\n')
head,rows=rows[0],[r for r in rows[1:] if r]
known=[r.split(',')[0] for r in rows]
chans=sorted(set(r.split(',')[1] for r in rows))
extra=['x','c9','c_0','c_x','C_12','c_007','c_9999','c_12345','c_99999','c_1234x','c_123456','c_1234567','c_12345678','c_123456789','c_1234567890','c_12345678901','camp_long_one','q7','zz_zz_z','c_2001','c_20000']
extra=[e for e in extra if e not in known]
with open(os.path.join(out,'campaigns.csv'),'w') as f:
    f.write(head+'\n')
    mixed=rows+[e+','+rnd.choice(chans) for e in extra]
    rnd.shuffle(mixed)
    for r in mixed:f.write(r+'\n')
def other():
    r=rnd.random()
    if r<0.40:return rnd.choice(extra)
    k=rnd.choice(known+extra)
    m=rnd.randrange(12)
    if m==0:return k[:-1]
    if m==1:return k+rnd.choice('0123456789x_ ')
    if m==2:i=rnd.randrange(len(k));return k[:i]+rnd.choice('0123456789cC_xX')+k[i+1:]
    if m==3:return k.upper()
    if m==4:return ' '+k
    if m==5:return k+' '
    if m==6:return 'c_0'+k[2:] if k.startswith('c_') else k+k
    if m==7:return ''
    if m==8:return 'c_'+str(rnd.randrange(0,400000))
    if m==9:return ''.join(rnd.choice('c_0123456789') for _ in range(rnd.choice([1,2,6,7,8,9,11,12,13,20])))
    if m==10:return k+k
    return rnd.choice(['c','c_','_','0','c_1,','c__1','c_1_','c-1'])
changed=0;lines=0;pick={} # click_id -> (new text, quoted when it holds a comma): copies of a click must stay copies
for name in sorted(os.listdir(os.path.join(base,'clicks'))):
    src=open(os.path.join(base,'clicks',name),newline='').read().split('\n')
    res=[src[0]]
    for line in src[1:]:
        f=line.split(',')
        if len(f)==4:
            cid=f[0].strip('"')
            if cid not in pick:pick[cid]=(other(),rnd.random()<0.5) if rnd.random()<0.2 else None
            if pick[cid]:
                t,qc=pick[cid]
                if ',' in t:f[2]='"'+t+'"' if qc else t # unquoted: a line of five fields, in every copy
                else:f[2]='"'+t+'"' if rnd.random()<0.5 else t
                line=','.join(f);changed+=1
        lines+=1;res.append(line)
    open(os.path.join(out,'clicks',name),'w',newline='').write('\n'.join(res))
print('camp_sets: %d campaigns (%d extra), %d of %d click lines changed'%(len(known)+len(extra),len(extra),changed,lines))
