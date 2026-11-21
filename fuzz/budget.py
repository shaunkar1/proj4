#!/usr/bin/env python3
# usage: budget.py CALLGRIND_OUT SOURCE FUNCTION_SUBSTRING ROWS
# Instructions of one function (inlined code included, calls listed separately) by source line, read
# straight from the callgrind file so that helpers shared with other functions are split correctly.
import sys,re,collections
out,src,fsub,rows=sys.argv[1],sys.argv[2],sys.argv[3],float(sys.argv[4])
names={}  # (kind,id)->name
def nm(kind,tok):
    m=re.match(r'\((\d+)\)(?: (.*))?$',tok)
    if not m: return tok
    if m.group(2) is not None: names[(kind,m.group(1))]=m.group(2)
    return names.get((kind,m.group(1)),'?')
cost=collections.Counter(); calls=collections.Counter()
fn=None; cur=None; last=0; skip=False; callee=None
for l in open(out,errors='replace'):
    l=l.rstrip('\n')
    if not l: continue
    c=l[0]
    if l.startswith('fl='): fl=nm('f',l[3:]); cur=fl; continue
    if l.startswith('fi=') or l.startswith('fe='): cur=nm('f',l[3:]); continue
    if l.startswith('fn='): fn=nm('n',l[3:]); last=0; continue
    if l.startswith('cfn='): callee=nm('n',l[4:]); continue
    if l.startswith('cfi=') or l.startswith('cfl='): nm('f',l[4:]); continue
    if l.startswith('calls='): skip=True; continue
    if c.isdigit() or c in '+-*':
        p=l.split()
        if c=='+': ln=last+int(p[0][1:])
        elif c=='-': ln=last-int(p[0][1:])
        elif c=='*': ln=last
        else: ln=int(p[0])
        last=ln
        v=int(p[1]) if len(p)>1 else 0
        if fn and fsub in fn:
            if skip: calls[callee.split('(')[0][:60]]+=v
            else: cost[(cur,ln)]+=v
        skip=False
srclines=open(src).read().split('\n')
tot=sum(cost.values()); ctot=sum(calls.values())
print('function *%s*: own %d (%.1f per row), calls %d (%.1f per row)'%(fsub,tot,tot/rows,ctot,ctot/rows))
byfile=collections.Counter()
for (f,ln),v in cost.items(): byfile[f.split('/')[-1]]+=v
print('by file:',', '.join('%s %.1f'%(k,v/rows) for k,v in byfile.most_common(12)))
print('calls:',', '.join('%s %.1f'%(k,v/rows) for k,v in calls.most_common(8)))
for (f,ln),v in sorted(cost.items(),key=lambda x:-x[1])[:int(sys.argv[5]) if len(sys.argv)>5 else 60]:
    text=srclines[ln-1].strip()[:110] if f.endswith(src.split('/')[-1]) and ln-1<len(srclines) else ''
    print('%7.2f  %s:%d  %s'%(v/rows,f.split('/')[-1],ln,text))
