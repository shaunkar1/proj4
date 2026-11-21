#!/usr/bin/env python3
# usage: lines.py CG_OUT SOURCE PATTERN [MIN]  -- per-line instruction counts (callgrind) of the source lines
# from the first line containing PATTERN to the end of that function (a line that is just "}").
import subprocess,sys,re
out,src,pat=sys.argv[1:4]; mn=int(sys.argv[4]) if len(sys.argv)>4 else 200000
t=subprocess.run(['callgrind_annotate','--auto=yes','--show-percs=no',out,src],capture_output=True,text=True).stdout
on=False
for bl in t.split('-- Auto-annotated source: ')[1:]:
    if not bl.split('\n',1)[0].strip().endswith(src.split('/')[-1]): continue
    for l in bl.split('\n'):
        m=re.match(r'^\s*([0-9.][0-9,]*)\s\s?(.*)$',l)
        if not m: continue
        body=m.group(2)
        if pat in body and not body.lstrip().startswith('=>'): on=True
        if on:
            c=m.group(1).replace(',','')
            if c!='.' and int(c)>=mn: print('%11s  %s'%(m.group(1),body[:170]))
            if body.rstrip()=='}': on=False
