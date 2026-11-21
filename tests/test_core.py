"""Synthetic correctness tests for the compiled core (not official Buildathon fixtures)."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import csv
import json
import os
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
BIN=ROOT/'shopping-core'

def create(root, *, start, end, links, events, clicks, products=None, campaigns=None):
    root.mkdir(parents=True,exist_ok=True)
    for part in ('links','events','clicks'):(root/part).mkdir()
    (root/'manifest.json').write_text(json.dumps({'window_start':start,'window_end':end}))
    def put(path,header,lines):
        # Keep raw CSV text including quotes and CR. Non-raw tuples are joined without quoting.
        with path.open('wb') as f:
            f.write((header+'\n').encode())
            for l in lines:
                if isinstance(l,(tuple,list)):l=','.join(l)
                f.write((l+'\n').encode('utf8'))
    put(root/'links'/'part.csv','user_id_a,user_id_b',links)
    put(root/'events'/'part.csv','event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents',events)
    put(root/'clicks'/'part.csv','click_id,user_id,campaign_id,ts',clicks)
    put(root/'products.csv','product_id,category',products or [('p1','books'),('p2','electronics')])
    put(root/'campaigns.csv','campaign_id,channel',campaigns or [('cA','ads'),('cB','social')])

def run_core(data, scratch):
    env=dict(os.environ,DATA_DIR=str(data),SCRATCH_DIR=str(scratch),OUTPUT_DIR=str(scratch/'out'),SS_DEBUG_CSV='1')
    p=subprocess.run([str(BIN)],env=env,capture_output=True,text=True)
    if p.returncode:raise RuntimeError(p.stderr)
    def parse(path):
        with (scratch/path).open(newline='') as f:
            rows=list(csv.reader(f))
        return rows
    u={}
    for h,day,sess,ev,rev,cat in parse('user_rows.csv'):
        key=(bytes.fromhex(h).decode(),int(day))
        u[key]=(int(sess),int(ev),int(rev),None if cat=='NULL' else bytes.fromhex(cat).decode())
    c={}
    for h,ch,day,s,p,r,a in parse('campaign_rows.csv'):
        key=(bytes.fromhex(h).decode(),int(day))
        c[key]=(bytes.fromhex(ch).decode(),int(s),int(p),int(r),int(a))
    return u,c

class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not BIN.exists():
            subprocess.run(['g++','-std=c++20','-O2','-fopenmp','-o',str(BIN),str(ROOT/'main.cpp')],check=True)

    def test_sessions_attribution_assists_and_bad_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);d=root/'data';s=root/'scratch'
            create(d,start='2026-10-01T00:00:00Z',end='2026-10-03T00:00:00Z',
                links=[('b','a'),('a','z'),('ignored','ignored')],
                events=[
                    ('e1','b','2026-10-01T23:59:00Z','2026-10-02T00:00:00Z','purchase','p1','100'),
                    ('e2','a','2026-10-02T00:10:00Z','2026-10-02T00:11:00Z','view','p1','junk'),
                    ('e3','a','2026-10-02T00:40:00Z','2026-10-02T00:42:00Z','cart','p2',''),
                    ('e4','a','2026-10-02T01:10:00.001Z','2026-10-02T01:12:00Z','view','p2','junk'),
                    ('e4','a','1790903400001','2026-10-02T01:11:00Z','view','p2','junk'),
                    ('e5','z','2026-10-02T02:00:00Z','2026-10-02T02:01:00Z','purchase','p2','0500'),
                    ('ebad','a','garbage','2026-10-02T02:00:00Z','view','p1',''),
                    ('ebad','a','2026-10-02T03:00:00Z','2026-10-02T03:01:00Z','view','p1',''),
                ],clicks=[
                    ('c1','b','cA','2026-09-24T23:59:00Z'),
                    ('x2','a','cB','2026-10-01T23:59:00Z'),
                    ('x1','a','cA','2026-10-01T23:59:00Z'),
                    ('x1','a','cA','2026-10-01T23:59:00Z'),
                    ('c3','a','cB','2026-10-02T00:20:00Z'),
                ])
            u,c=run_core(d,s)
            o1=int(datetime(2026,10,1,tzinfo=timezone.utc).timestamp()//86400)
            self.assertEqual(u, {('a',o1):(1,3,100,'books'),('a',o1+1):(2,2,500,'electronics')})
            self.assertEqual(c[('cA',o1)],('ads',1,1,100,1))
            self.assertEqual(c[('cB',o1)],('social',0,0,0,1))
            self.assertEqual(c[('cB',o1+1)],('social',2,1,500,2))
            self.assertEqual(c[('cA',o1+1)],('ads',0,0,0,2))

    def test_bad_price_timestamp_link_and_exact_campaign_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);d=root/'data';s=root/'scratch'
            create(d,start='2028-02-28T00:00:00Z',end='2028-03-02T00:00:00Z',
                links=['"A","B"','u,v\r',',', 'B,C'],
                events=[
                    ('a','B','2028-02-29T12:00:00Z','2028-02-29T13:00:00Z','purchase','gone','10'),
                    ('b','C','2028-02-29T12:30:00Z','2028-02-29T13:00:00Z','view','p1','1000\r'),
                    ('c','C','2028-02-29T13:00:00Z','2028-02-29T14:00:00Z','purchase','p1','20'),
                    ('d','C','2028-02-29T13:30:00Z','2028-02-29T14:00:00Z','purchase','p2','0'),
                    ('e','C','2028-02-30T00:00:00Z','2028-02-29T14:00:00Z','view','p1',''),
                    ('f','C','2028-02-29T16:00:00+03:00','2028-02-29T14:00:00Z','view','p1',''),
                    ('g','u','2028-02-29T12:00:00Z','2028-02-29T12:00:00Z','view','p2',''),
                    ('h','v\r','2028-02-29T12:00:00Z','2028-02-29T12:00:00Z','view','p2','')
                ],clicks=[
                    ('click','C','cB','2028-02-22T12:00:00Z'),
                    ('future','C','cA','2028-02-29T12:00:00.001Z'),
                ])
            u,c=run_core(d,s)
            day=int(datetime(2028,2,29,tzinfo=timezone.utc).timestamp()//86400)
            # one session from 12:00 to 13:00, with an invalid purchase at 13:30
            self.assertEqual(u[('A',day)],(1,4,30,'books'))
            # u/v\r link only if CR preserved, and u<v\r, separate person from A
            self.assertEqual(u[('u',day)],(1,2,0,None))
            self.assertEqual(c[('cB',day)][1:],(1,1,30,1))
            self.assertNotIn(('cA',day),c)


    def test_organic_top_category_tie_and_earliest_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);d=root/'data';scratch=root/'scratch'
            create(d,start='2026-10-01T00:00:00Z',end='2026-10-03T00:00:00Z',
                links=[('z','u'),('A','z'),('orphan','A')],
                products=[('a','zz'),('b','aa')],
                events=[
                    ('first','u','2026-10-01T02:00:00Z','2026-10-01T04:00:00Z','purchase','a','00100'),
                    ('second','z','2026-10-01T02:05:00Z','2026-10-01T04:00:00Z','purchase','b','100'),
                    ('n','u','2026-10-01T02:10:00Z','2026-10-01T03:00:00Z','view','a','-17'),
                    ('dup','u','bad','2026-10-01T05:00:00Z','view','a',''),
                    ('dup','u','2026-10-01T03:00:00Z','2026-10-01T06:00:00Z','purchase','a','999'),
                    ('late','z','2026-10-01T03:00:00Z','2026-10-02T03:00:00Z','view','a','8'),
                    ('toLate','z','2026-10-01T03:00:01Z','2026-10-02T03:00:02Z','purchase','a','8'),
                    ('unknown','u','2026-10-01T03:00:00Z','2026-10-01T04:00:00Z','purchase','missing','50'),
                    ('invalid_price','u','2026-10-01T03:00:00Z','2026-10-01T04:00:00Z','purchase','a','٢٥٠'),
                    ('badcolumns','u','2026-10-01T03:00:00Z','2026-10-01T04:00:00Z','purchase','a','100','extra'),
                    'badquote,u,2026-10-01T03:00:00Z,2026-10-01T04:00:00Z,"purchase"x,a,5',
                    ('other','solo','2026-10-01T03:00:00Z','2026-10-01T04:00:00Z','view','a',''),
                ], clicks=[])
            u,c=run_core(d,scratch)
            day=int(datetime(2026,10,1,tzinfo=timezone.utc).timestamp()//86400)
            # two main-person sessions: 2:00 / 3:00, second contains late-at-24h plus unknown
            self.assertEqual(u[('A',day)],(2,5,250,'aa'))
            self.assertEqual(u[('solo',day)],(1,1,0,None))
            self.assertEqual(c[('organic',day)],('organic',3,2,250,0))
            self.assertEqual(len(c),1)

    def test_dense_hash_rehash_and_zero_ingest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);d=root/'data';s=root/'scratch'
            records=[(str(i),'u','1000','1000','view','p1','') for i in range(20000)]
            # Earlier record for duplicate ID, including zero-ingest sentinel.
            records.append(('0','u','1000','1001','view','p1',''))
            create(d,start='1970-01-01T00:00:00Z',end='1970-01-03T00:00:00Z',
                   links=[],events=records,clicks=[])
            u,c=run_core(d,s)
            self.assertEqual(u,{('u',0):(1,20000,0,None)})
            self.assertEqual(c[('organic',0)][1:],(1,0,0,0))

    def test_equal_timestamp_click_tie_and_7d_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);d=root/'data';s=root/'scratch'
            create(d,start='2026-10-01T00:00:00Z',end='2026-10-03T00:00:00Z',links=[],events=[
                ('e1','u1','2026-10-01T10:00:00Z','2026-10-01T10:00:00Z','purchase','p1','100'),
                ('e2','u1','2026-10-01T11:00:00Z','2026-10-01T11:00:00Z','view','p1',''),
                ('e3','u1','2026-10-01T12:00:00Z','2026-10-01T12:00:00Z','view','p1',''),
            ], clicks=[
                ('z','u1','cA','2026-10-01T10:00:00Z'),
                ('a','u1','cB','2026-10-01T10:00:00Z'),
                ('q','u1','cA','2026-09-24T10:00:00Z'),
                ('too_old','u1','cB','2026-09-24T09:59:59Z'),
                ('future','u1','cA','2026-10-01T12:00:01Z'),
            ])
            u,c=run_core(d,s)
            day=int(datetime(2026,10,1,tzinfo=timezone.utc).timestamp()//86400)
            self.assertEqual(u[('u1',day)],(3,3,100,'books'))
            self.assertEqual(c[('cB',day)],('social',3,1,100,3))
            self.assertEqual(c[('cA',day)],('ads',0,0,0,3))

if __name__=='__main__':unittest.main()