#!/usr/bin/env python3
# usage: bdiff.py BUDGET_A BUDGET_B [N]  -- the lines whose instructions per row changed most between two budget.py listings
import re, sys
def load(fn):
    d = {}
    for l in open(fn).read().split('\n')[4:]:
        m = re.match(r'\s*([\d.]+)\s+(\S+):(\d+)\s*(.*)', l)
        if m:
            k = (m.group(2), m.group(4)[:78] if m.group(4) else m.group(3)); d[k] = d.get(k, 0) + float(m.group(1))
    return d
a, b = load(sys.argv[1]), load(sys.argv[2]); n = int(sys.argv[3]) if len(sys.argv) > 3 else 30
rows = [(b.get(k, 0) - a.get(k, 0), a.get(k, 0), b.get(k, 0), k) for k in set(a) | set(b)]
for r in sorted(rows, key=lambda r: -abs(r[0]))[:n]: print('%+7.2f  %6.2f -> %6.2f  %s  %s' % (r[0], r[1], r[2], r[3][0], r[3][1]))
print('total diff %.1f' % sum(r[0] for r in rows))
