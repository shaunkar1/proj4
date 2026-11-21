#!/usr/bin/env python3
# usage: gen_links.py SRC_DATA OUT_DATA SEED -- dev data with mutated links / products / campaigns files
# (events and clicks are linked in unchanged), for differential tests of the start-up and links stages.
import sys,os,random,glob,shutil
src,out,seed=sys.argv[1],sys.argv[2],int(sys.argv[3])
R=random.Random(seed)
if os.path.isdir(out): shutil.rmtree(out)
for d in ('events','clicks','links'): os.makedirs(os.path.join(out,d))
shutil.copy(os.path.join(src,'manifest.json'),os.path.join(out,'manifest.json'))
for d,k in (('events',3),('clicks',3)):
    for f in sorted(glob.glob(os.path.join(src,d,'*.csv')))[:k]: os.symlink(f,os.path.join(out,d,os.path.basename(f)))
def q(x): return b'"'+x+b'"'
def rid(n): return bytes(R.choice(b'abcdefghijklmnopqrstuvwxyzABC0123456789_') for _ in range(n))
JUNK=[b'"',b'""',b'"""',b'a"b',b'"a',b'a"',b'"a"b"',b' ',b'\r',b'\x00',b'\t',b'"\r"',b' "x"',b'"x" ',b'x'*12,b'y'*15,b'z'*16,b'w'*17,b'v'*40,b'U1',b'']
def mut_pair(f,ids):
    try: return mut_pair1(f,ids)
    except (IndexError,ValueError): return list(f)
def mut_pair1(f,ids):
    f=list(f); k=R.random()
    if k<0.15: i=R.randrange(len(f)); f[i]=q(f[i])
    elif k<0.25: f=[q(x) for x in f]
    elif k<0.35: f[R.randrange(len(f))]=R.choice(JUNK)
    elif k<0.45: f[R.randrange(len(f))]=R.choice(ids)
    elif k<0.55: f[R.randrange(len(f))]=b'U'+rid(R.choice([0,1,6,7,8,9,10,11,12,14,15,16,17,24,33,60]))
    elif k<0.62: f[R.randrange(len(f))]+=R.choice([b'\r',b' ',b'\x00'])
    elif k<0.68: f.insert(R.randrange(len(f)+1),R.choice([b'',b'x',b'"x"']))
    elif k<0.74: del f[R.randrange(len(f))]
    elif k<0.80: f=[b'']*R.choice([0,1,2,3])
    elif k<0.86: f[R.randrange(len(f))]=q(q(f[R.randrange(len(f))]))
    elif k<0.93: f=[f[-1],f[0]]
    else: f[1]=f[0]
    return f
ids=[]
for f in sorted(glob.glob(os.path.join(src,'links','*.csv'))):
    for l in open(f,'rb').read().split(b'\n')[1:400]:
        ids+=[x for x in l.split(b',') if x]
longids=[b'L'+rid(R.choice([11,12,14,15,16,20,40])) for _ in range(30)]
for n,f in enumerate(sorted(glob.glob(os.path.join(src,'links','*.csv')))[:R.choice([1,3,6])]):
    data=open(f,'rb').read().split(b'\n'); hdr=data[0]; lines=[l for l in data[1:] if l]
    rate=R.choice([0.02,0.2,0.7]); outl=[R.choice([hdr,hdr,b'',b'a,b',b'"x"'])]
    for l in lines[:R.randrange(1,len(lines)+1)]:
        if R.random()<rate:
            p=l.split(b',')
            if R.random()<0.2: p[R.randrange(len(p))]=R.choice(longids)
            p=mut_pair(p,ids)
            if R.random()<0.2: p=mut_pair(p,ids)
            l=b','.join(p)
        outl.append(l)
        if R.random()<0.01: outl.append(b'')
    open(os.path.join(out,'links','part-%05d.csv'%n),'wb').write(b'\n'.join(outl)+R.choice([b'\n',b'',b'\n\n',b'\r\n',b'\n'+b'x'*R.randrange(40)]))
if R.random()<0.2: open(os.path.join(out,'links','part-00009.csv'),'wb').write(R.choice([b'',b'a,b',b'a,b\n',b'a,b\nU1,U2',b'\n',b'a,b\n,\n"",""\nU1,"U2"\n']))
bad=R.random()<0.1   # a malformed small file must make the program fail, the same way in both builds
for name in ('products.csv','campaigns.csv'):
    data=open(os.path.join(src,name),'rb').read().split(b'\n'); hdr=data[0]; lines=[l for l in data[1:] if l]; outl=[hdr]
    for l in lines:
        r=R.random(); p=l.split(b',')
        if r<0.03: p=[q(x) for x in p]
        elif r<0.06: i=R.randrange(2); p[i]=q(p[i])
        elif r<0.08: p[1]=R.choice([b'',b'cat\r',b' ',b'"x"',b'averyveryverylongcategoryname',b'unknown',b'electro',b'\x00'])
        elif r<0.10: p[0]=R.choice([b'',b'p1',b'"p2"',b'p13649',b'productwithaverylongname'])
        elif r<0.11 and len(outl)>1: p[0]=outl[-1].split(b',')[0]
        outl.append(b','.join(p[:2]) if len(p)>=2 else l)
    if bad and name==R.choice(['products.csv','campaigns.csv']):
        outl.insert(R.randrange(1,len(outl)+1),R.choice([b'',b'a',b'a,b,c',b'a"b,c',b'"a,b',b'a,"b',b'""a,b']))
    open(os.path.join(out,name),'wb').write(b'\n'.join(outl)+R.choice([b'\n',b'',b'\r\n']))
