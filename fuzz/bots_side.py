#!/usr/bin/env python3
"""General-lane rows of OTHER aliases for marked user indexes (test set for the index pass's marked indexes).

usage: bots_side.py BASE OUT N KU [SEED=1]      (KU: the user key, second number of the 'STATS dense keys' line)

Copies BASE and adds one event file and one click file with N extra rows each. Every extra row is a copy of a
row of BASE with a new, odd ID (upper-case letter, so it goes through the general lane and marks its user
index) and with the user's alias changed to another alias 0..7 of the same index: where the two aliases are not
linked, the index then holds a person that exists only in the general lane next to the person of its slots, and
must not be finished as one person. Every third chosen row gets two such rows with two different other aliases.
Event copies are 1 s later than their row; click copies keep time and campaign (so they tie with their row).
A third group (N/2 indexes, see side_only) makes persons that have events in the general lane only and a click in
the slots: the general lane must then be given that click."""
import os, random, shutil, sys
base, out, N, KU = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
R = random.Random(int(sys.argv[5]) if len(sys.argv) > 5 else 1)
MASK = (1 << 40) - 1; M1 = 0x9E3779B97F4B & MASK; M2 = 0xD6E8FEB86659 & MASK
def inv40(a):
    x = a
    for _ in range(6): x = (x * (2 - a * x)) & ((1 << 64) - 1)
    return x & MASK
I1 = inv40(M1); I2 = inv40(M2)
D = "0123456789abcdefghijklmnopqrstuvwxyz"
def b36(v):
    s = ""
    while True:
        s = D[v % 36] + s; v //= 36
        if v == 0: return s
def value_of(i, k):
    m = ((i * M1) ^ k) & MASK; m ^= m >> 17; return (m * M2) & MASK
def index_of(y, k):
    m = (y * I2) & MASK; m = m ^ (m >> 17) ^ (m >> 34); return ((m ^ k) * I1) & MASK
def parse36(s):
    if not (1 <= len(s) <= 8) or (s[0] == '0' and len(s) > 1): return None
    v = 0
    for ch in s:
        d = D.find(ch)
        if d < 0: return None
        v = v * 36 + d
    return v if v <= MASK else None
shutil.copytree(base, out, dirs_exist_ok=True)
def extra(sub, nf, tscol, shift):
    files = sorted(os.listdir(os.path.join(base, sub)))
    rows = []
    for fn in files[:6]:
        with open(os.path.join(base, sub, fn), newline="", encoding="latin-1") as f:
            head = f.readline()
            for line in f:
                p = line.rstrip("\n").split(",")
                if len(p) != nf or '"' in line: continue
                y = parse36(p[1][1:]) if p[1][:1] == 'u' else None
                if y is None: continue
                c = index_of(y, KU)
                if (c & 511) < 8 and p[tscol].isdigit() and len(p[tscol]) == 13 and p[0][1:].isalnum(): rows.append((p, c))
    R.shuffle(rows)
    name = "part-%05d.csv" % len(files); n = 0
    with open(os.path.join(out, sub, name), "w", newline="", encoding="latin-1") as f:
        f.write(head)
        for i, (p, c) in enumerate(rows[:N]):
            others = [a for a in range(8) if a != (c & 511)]; R.shuffle(others)
            for j, a in enumerate(others[:2 if i % 3 == 0 else 1]):
                q = list(p)
                q[0] = p[0][0].upper() + p[0][1:] + "x%d" % j
                q[1] = "u" + b36(value_of((c >> 9 << 9) | a, KU))
                q[tscol] = str(int(p[tscol]) + shift)
                f.write(",".join(q) + "\n"); n += 1
    print(f"{sub}: {n} rows in {name}")
extra("events", 7, 2, 1000)
extra("clicks", 4, 3, 0)
def side_only(n_want):
    """A person that has events in the general lane only, and a click in the slots: an event row is copied with an
    odd ID and another alias (as above), and one dense click of the same index is given that alias and a time
    one hour before the event, in every line that carries the click's ID."""
    cdir = os.path.join(base, "clicks"); by_index = {}; lines = {}
    for fn in sorted(os.listdir(cdir)):
        with open(os.path.join(cdir, fn), newline="", encoding="latin-1") as f: lines[fn] = f.read().split("\n")
        for line in lines[fn][1:]:
            p = line.split(",")
            if len(p) != 4 or '"' in line or p[1][:1] != 'u' or p[0][:1] != 'k' or parse36(p[0][1:]) is None: continue
            y = parse36(p[1][1:])
            if y is None: continue
            c = index_of(y, KU)
            if (c & 511) < 8: by_index.setdefault(c >> 9, set()).add(p[0])
    efiles = sorted(os.listdir(os.path.join(base, "events"))); cand = []
    for fn in efiles[6:12]:
        with open(os.path.join(base, "events", fn), newline="", encoding="latin-1") as f:
            head = f.readline()
            for line in f:
                p = line.rstrip("\n").split(",")
                if len(p) != 7 or '"' in line or p[1][:1] != 'u': continue
                y = parse36(p[1][1:])
                if y is None: continue
                c = index_of(y, KU)
                if (c & 511) < 8 and p[2].isdigit() and len(p[2]) == 13 and (c >> 9) in by_index: cand.append((p, c))
    R.shuffle(cand); change = {}; used = set(); n = 0
    with open(os.path.join(out, "events", "part-%05d.csv" % (len(efiles) + 1)), "w", newline="", encoding="latin-1") as f:
        f.write(head)
        for p, c in cand:
            if n >= n_want: break
            ix = c >> 9
            if ix in used: continue
            a = R.choice([x for x in range(8) if x != (c & 511)]); user = "u" + b36(value_of((ix << 9) | a, KU))
            q = list(p); q[0] = p[0][0].upper() + p[0][1:] + "y"; q[1] = user; q[4] = "purchase" if n % 2 else q[4]
            f.write(",".join(q) + "\n")
            change[sorted(by_index[ix])[R.randrange(len(by_index[ix]))]] = (user, str(int(p[2]) - 3600000)); used.add(ix); n += 1
    for fn, ls in lines.items():
        hit = False
        for i, line in enumerate(ls):
            p = line.split(",")
            if len(p) == 4 and p[0].strip('"') in change:
                u, t = change[p[0].strip('"')]; ls[i] = ",".join([p[0], u, p[2], t]); hit = True
        if hit:
            with open(os.path.join(out, "clicks", fn), "w", newline="", encoding="latin-1") as f: f.write("\n".join(ls))
    print(f"side-only persons: {n} events, {len(change)} clicks re-aliased")
side_only(N // 2)
