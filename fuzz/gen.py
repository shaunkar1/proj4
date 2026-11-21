#!/usr/bin/env python3
# usage: gen.py SRC_DATA OUT_DATA SEED  -- writes a mutated copy of a few dev files for differential testing
import sys,os,random,glob,shutil
src,out,seed=sys.argv[1],sys.argv[2],int(sys.argv[3])
R=random.Random(seed)
NODUP=os.environ.get('FZ_NODUP')=='1'; NONUL=os.environ.get('FZ_NONUL')=='1'; UNIQ=os.environ.get('FZ_UNIQ')=='1'; uniq=[0]
for d in ('events','clicks','links'): os.makedirs(os.path.join(out,d),exist_ok=True)
for f in ('manifest.json','products.csv','campaigns.csv'): shutil.copy(os.path.join(src,f),os.path.join(out,f))
for f in sorted(glob.glob(os.path.join(src,'links','*.csv')))[:4]: shutil.copy(f,os.path.join(out,'links',os.path.basename(f)))
TS=[b'2027-12-09T00:00:00Z',b'2027-12-08T23:59:59Z',b'2027-12-23T00:00:00Z',b'2027-12-22T23:59:59Z',b'2027-12-22T23:59:60Z',
 b'2027-12-10T24:00:00Z',b'2027-12-10T23:60:00Z',b'2027-12-10T29:00:00Z',b'2027-13-10T10:00:00Z',b'2027-12-32T10:00:00Z',b'2027-02-29T10:00:00Z',
 b'2028-02-29T10:00:00Z',b'2027-12-10t10:00:00Z',b'2027-12-10T10:00:00z',b'2027-12-10 10:00:00Z',b'2027/12/10T10:00:00Z',b'2027-12-10T10-00-00Z',
 b'2027-12-1010:00:00ZZ',b'20271210101010101010',b'99999999999999999999',b'00000000001828000000',b'2027-12-10T10:00:00',b'2027-12-10T10:00:0Z',
 b'2027-12-10T10:00:00+00:00',b'2027-12-10T10:00:00-00:00',b'2027-12-10T10:00:00+23:59',b'2027-12-10T10:00:00-23:59',b'2027-12-10T10:00:00+24:00',
 b'2027-12-10T10:00:00+05:60',b'2027-12-10T10:00:00*05:00',b'2027-12-10T10:00:00+0500Z',b'2027-12-10T10:00:00+5:000',b'2027-12-09T02:00:00+05:00',
 b'2027-12-22T22:00:00-05:00',b'2027-12-10T1a:00:00+05:00',b'2027-00-10T10:00:00+05:00',b'1828396800000',b'1828396799999',b'1829606400000',b'1829606399999',
 b'182839680000',b'18283968000000',b'0001828400000',b'182840000000a',b'18284 0000000',b'1828400000000000',b'001828400000000000',b'0000001828400000000',
 b'9223372036854775807',b'9223372036854775808',b'18446744073709551616',b'-1828400000000',b'+1828400000000',b'',b' ',b'0',b'1828400000000 ',
 b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00T10:00:00Z',b'2027-12-10T10:00:00\xc3\xbf',b'2027-12-\xc2\xb0T10:00:00Z',b'\xc2\xb227-12-10T10:00:00Z',
 b'2027-12-10T\xc2\xb1:00:00Z',b'2027-12-10T10:00:\xc2\xb5Z',b'0000-01-01T00:00:00Z',b'9999-12-31T23:59:59Z',b'1970-01-01T00:00:00Z',b'2027-12-15T12:34:56Z',
 b'2027-11-30T12:34:56Z',b'2027-12-1AT10:00:00Z',b'2027-1B-10T10:00:00Z',b'202G-12-10T10:00:00Z',b'2027-12-10T10:00:00+0A:00',b'2027-12-10T10:00:00+00:0A']
if NONUL: TS=[t for t in TS if not t.startswith(b'\x00')]
def rid(n): return bytes(R.choice(b'abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(n))
def junk(): return R.choice([b'"',b'""',b'"""',b'""""',b'a"b',b'"a',b'a"',b'"a"b"',b' ',b'\r',b'\x00',b'*',b'(',b'.',b'\t',b'"\r"',b' "x"',b'"x" ',b'x'*R.choice([12,15,16,17,31,40,70,130]),b'\xc3\xa9',b'\xe2\x82\xac'])
def q(f): return b'"'+f+b'"'
def mutate(f,kind):
    try: return mutate1(list(f),kind)
    except (IndexError,ValueError): return f
def mutate1(f,kind):
    n=len(f); k=R.random()
    tsidx=[2,3] if kind=='e' else [3]
    if k<0.22:
        i=R.choice(tsidx); f[i]=R.choice(TS)
    elif k<0.30:
        i=R.randrange(n); f[i]=junk()
    elif k<0.38:
        i=R.choice([0,1]); f[i]=(f[i][:1] if f[i][:1].isalnum() else b'x')+rid(R.choice([0,1,5,6,7,8,9,10,11,12,14,15,16,17,24,33,60,120]))
    elif k<0.42:
        f[R.randrange(n)]=b''
    elif k<0.47 and kind=='e':
        f[4]=R.choice([b'purchase',b'purchase',b'view',b'cart',b'View',b'view ',b'purchas',b'purchases',b'PURCHASE',b'vieW',b'tart',b'',b'refund'])
        f[6]=R.choice([b'',b'0',b'1',b'00012',b'10000000',b'10000001',b'9999999',b'12a',b'-5',b' 5',b'5 ',b'1e3',b'99999999999999999999',b'0000000000000000000000001',b'12.5'])
    elif k<0.50 and kind=='e':
        f[5]=R.choice([b'',b'p1',b'p999999',b'P13649',b'p13649 ',b'p13649',b'productwithaverylongname'])
    elif k<0.53 and kind=='c':
        f[2]=R.choice([b'',b'c_1',b'c_999999',b'C_1190',b'c_1190 ',b'campaignwithaverylongname'])
    elif k<0.60:
        del f[R.randrange(n)]
    elif k<0.66:
        f.insert(R.randrange(n+1),R.choice([b'',b'x',b'"x"']))
    elif k<0.70:
        return [b'']*R.choice([0,1,2,n,n+1]) if R.random()<0.5 else [R.choice([b'',b'"',b'\r',b' ',b'""'])]
    elif k<0.75:
        f[R.randrange(n)]+=R.choice([b'\r',b' ',b'\x00'])
    elif k<0.80:
        f=[q(x) for x in f]
    elif k<0.86 and kind=='e':
        f[2],f[3]=f[3],f[2]
    else:
        i=R.randrange(n); f[i]=q(f[i]) if R.random()<0.7 else q(q(f[i]))
    return f
def strip(x): return x[1:-1] if len(x)>=2 and x[:1]==b'"' and x[-1:]==b'"' else x
def proc(path,dst,kind,rate):
    data=open(path,'rb').read().split(b'\n'); hdr=data[0]; lines=[l for l in data[1:] if l]
    if len(lines)>1: lines=lines[:R.randrange(len(lines)//2,len(lines))]
    outl=[hdr]; prev=None
    for l in lines:
        r=R.random()
        if r<rate:
            f=l.split(b','); f=mutate(f,kind)
            if R.random()<0.3: f=mutate(f,kind)
            if UNIQ and f:   # a changed line must not collide with an unchanged copy of itself (same ID, other content: which one wins is order-dependent)
                uniq[0]+=1; f[0]=(b'kz%07d' if kind=='c' else b'ez%07d')%uniq[0]
            l=b','.join(f)
        elif r<rate+0.03 and prev is not None and not NODUP:
            f=prev.split(b','); g=l.split(b',')   # same id, other content: exercises dedup
            if len(f)==len(g): g[0]=f[0]; l=b','.join(g)
        elif r<rate+0.05 and prev is not None:
            l=prev
        outl.append(l); prev=l
    tail=R.choice([b'\n',b'',b'\n\n',b'\r\n'])
    open(dst,'wb').write(b'\n'.join(outl)+tail)
ev=sorted(glob.glob(os.path.join(src,'events','*.csv'))); cl=sorted(glob.glob(os.path.join(src,'clicks','*.csv')))
R.shuffle(ev); R.shuffle(cl)
for i,f in enumerate(ev[:3]): proc(f,os.path.join(out,'events','part-%05d.csv'%i),'e',R.choice([0.05,0.3,0.8]))
for i,f in enumerate(cl[:3]): proc(f,os.path.join(out,'clicks','part-%05d.csv'%i),'c',R.choice([0.05,0.3,0.8]))
open(os.path.join(out,'events','part-00009.csv'),'wb').write(b'event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents'+R.choice([b'',b'\n',b'\ne1,u1,2027-12-10T10:00:00Z,2027-12-10T10:00:01Z,view,p1,']))
