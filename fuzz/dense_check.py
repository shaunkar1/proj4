#!/usr/bin/env python3
"""Checks the parse stage of the dense route (DENSE.md stage 3) with the SS_DENSE_DUMP text dumps.

usage: dense_check.py CORE DATA_DIR WORK_DIR [K=V ...]      (K=V: extra environment for the dense runs,
                                                             e.g. SS_TEST_LIME=5000 SS_DSHIFT=6)
Runs CORE on DATA_DIR
  A  with SS_NODENSE=1, SS_ISA=sse2, one thread            (the reference: every line through the general handlers)
  B  with the dense switches on, SS_ISA=sse2, one thread
  V  with the dense switches on, every vector ISA and sse2, one and three threads
and checks
  1. rows of B == rows of A as multisets of (event_id, ingest, k, cat, user ID text) and (click_id, key, user ID text);
     A has no dense record and no dense user code
  2. in B the route of every ID is the one an independent decoder (below) gives for its text: an event / click
     ID takes the dense route iff it is letter + canonical base 36 with a counter below the limit; a user ID
     gets a dense code iff it is canonical, alias < 8, index below the limit and the index is not complex;
     no ID text appears on both routes
  3. a dense user code regenerates the user ID text; a table number is the number the ID table has for the text
  4. the records of B (texts rebuilt from the records alone) == the rows of B
  5. the records of every V run == the records of B (multisets; table users are named by their person)
Prints DENSE_PARSE_OK or the first differences and exits 1.
Every run writes its dump into the same folder WORK_DIR/dump and is read before the next run overwrites it,
so the scratch space is one dump (dev: about 130 MB), whatever the number of runs. WORK_DIR may be reused."""
import os, subprocess, sys
from collections import Counter

core, data, work = sys.argv[1], sys.argv[2], sys.argv[3]
extra = dict(a.split("=", 1) for a in sys.argv[4:])
FALLBACK = "SS_TEST_MAXNEW" in extra
LIGHT = os.environ.get("DC_LIGHT") == "1"   # big data: compare multisets by count and sum of line hashes (no listing of differences)

class HashBag:
    def __init__(self): self.n = 0; self.s = 0
    def add(self, x): self.n += 1; self.s = (self.s + hash(x)) & ((1 << 64) - 1)
    def __eq__(self, o): return self.n == o.n and self.s == o.s
class ListBag(Counter):
    def add(self, x): self[x] += 1
Bag = HashBag if LIGHT else ListBag
MASK = (1 << 40) - 1
M1 = 0x9E3779B97F4B & MASK; M2 = 0xD6E8FEB86659 & MASK
I1 = pow(M1, -1, 1 << 40); I2 = pow(M2, -1, 1 << 40)
DIG = {c: i for i, c in enumerate(b"0123456789abcdefghijklmnopqrstuvwxyz")}
bad = 0

def fail(msg):
    global bad
    bad += 1
    if bad <= 25: print("FAIL:", msg)

def index_of(y, key):
    m = (y * I2) & MASK; m ^= (m >> 17) ^ (m >> 34)
    return ((m ^ key) * I1) & MASK

def counter(text, letter, key):
    """Counter of an ID text (bytes) if it is letter + canonical base 36 below 2^40, else None."""
    if len(text) < 2 or len(text) > 9 or text[0] != letter: return None
    if text[1] == 0x30 and len(text) > 2: return None
    v = 0
    for c in text[1:]:
        d = DIG.get(c)
        if d is None: return None
        v = v * 36 + d
    if v > MASK: return None
    return index_of(v, key)

def run(name, env, threads):
    global THREADS
    THREADS = threads
    out = os.path.join(work, "dump")
    e = dict(os.environ); e.update(env)
    for k in ("SS_NODENSE", "SS_DENSE_USERS", "SS_DENSE_RECORDS", "SS_ISA"):
        if k not in env: e.pop(k, None)
    o = os.path.join(work, "out"); os.makedirs(o, exist_ok=True)
    e.update(DATA_DIR=data, OUTPUT_DIR=o, SCRATCH_DIR=o, NCPU=str(threads), SS_DENSE_DUMP=out, SS_STATS="1")
    p = subprocess.run([core], env=e, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if p.returncode != 0: sys.exit("run %s failed with code %d" % (name, p.returncode))
    keys = None; on = [0, 0]
    for line in p.stderr.decode("latin-1").split("\n"):
        if line.startswith("STATS dense keys"):
            w = line.split(); keys = [int(x) for x in w[3:6] + w[7:10]] + on
        elif line.startswith("dense: events"):     # whether the keys were found at all (printed before the STATS line)
            w = line.split(); on = [int(w[2]), int(w[4])]
    return out, keys

def lines(d, kind):
    for t in range(THREADS):     # only this run's files: an earlier run with more threads may have left others
        with open(os.path.join(d, "%s-%d.txt" % (kind, t)), "rb") as f:
            for ln in f: yield ln.rstrip(b"\n").split(b"\t")

def users(d):
    """ID table of a run: text -> number, number -> name of the person (its smallest text)."""
    num, name = {}, {}
    with open(os.path.join(d, "users.txt"), "rb") as f:
        for ln in f:
            n, t = ln.split()
            num[t] = n
            if n not in name or t < name[n]: name[n] = t
    return num, name

def label(desc, name):
    """What a record's user field stands for, in a form that does not depend on the run."""
    # With SS_TEST_MAXNEW the table fills up and the remaining new users get thread-local numbers; which users
    # still got a table number depends on the order the threads met them, so there both kinds are only "not dense".
    if FALLBACK and desc[:2] in (b"T:", b"L:"): return b"TL"
    if desc[:2] == b"T:": return b"T:" + name.get(desc[2:], b"?" + desc[2:])
    if desc[:2] == b"L:": return b"L"
    return desc

def recs(d):
    _, name = users(d)
    c = Bag()
    for f in lines(d, "recs"):
        c.add(tuple(f[:-1]) + (label(f[-1], name),))
    return c

def diff(what, a, b):
    if a == b: return
    if LIGHT: fail("%s: %d lines vs %d lines, or other lines" % (what, a.n, b.n)); return
    only_a, only_b = a - b, b - a
    fail("%s: %d lines only in the first, %d only in the second" % (what, sum(only_a.values()), sum(only_b.values())))
    for x in list(only_a)[:3]: print("   first :", x)
    for x in list(only_b)[:3]: print("   second:", x)

dense_env = dict(extra); dense_env.update(SS_DENSE_USERS="1", SS_DENSE_RECORDS="1")
A, _ = run("A-nodense-sse2", {"SS_NODENSE": "1", "SS_ISA": "sse2"}, 1)
rowsA, rowsB = Bag(), Bag()
for f in lines(A, "rows"):
    if f[1] != b"P": fail("dense record in the SS_NODENSE run: %r" % f)
    if f[-1][:2] == b"U:": fail("dense user in the SS_NODENSE run: %r" % f)
    rowsA.add((f[0],) + tuple(f[2:-1]))
B, keys = run("B-dense-sse2", dict(dense_env, SS_ISA="sse2"), 1)
if not keys: sys.exit("no 'STATS dense keys' line")
KE, KU, KC, LIME, LIMC, LIMI, ON_E, ON_C = keys
if not ON_E: LIME = 0          # keys not found for a column: nothing of it may take the dense route
if not ON_C: LIMC = 0
if not (ON_E or ON_C): LIMI = 0
with open(os.path.join(B, "complex.txt")) as f: complex_idx = set(int(x) for x in f.read().split())

# 1. same lines as the reference
numB, nameB = users(B)
route = {}; ukind = {}; rowsBl = Bag()
nE = nEd = nC = nCd = nU = nUd = nUcomplex = 0
for f in lines(B, "rows"):
    rowsB.add((f[0],) + tuple(f[2:-1]))
    idt = bytes.fromhex(f[2].decode())
    # 2. route of the ID
    if f[0] == b"E":
        c = counter(idt, 0x65, KE); want = b"D" if c is not None and c < LIME else b"P"
        nE += 1; nEd += want == b"D"
    else:
        c = counter(idt, 0x6B, KC); want = b"D" if c is not None and c < LIMC else b"P"
        nC += 1; nCd += want == b"D"
    if f[1] != want: fail("route %s, expected %s: %r" % (f[1].decode(), want.decode(), f))
    if not LIGHT and route.setdefault((f[0], f[2]), f[1]) != f[1]: fail("ID on both routes: %r" % f)   # (implied by the line above)
    # 2./3. the user
    desc, utext = f[-1], f[-2]
    if desc != b"-":
        c = counter(bytes.fromhex(utext.decode()), 0x75, KU)
        dense_u = c is not None and (c & 511) < 8 and (c >> 9) < LIMI
        if dense_u and (c >> 9) in complex_idx: dense_u = False; nUcomplex += 1
        nU += 1; nUd += dense_u
        if desc[:2] == b"U:":
            if not dense_u: fail("dense user code for a user that must use the table: %r" % f)
            if desc[2:] != utext: fail("dense user code regenerates another text: %r" % f)
        elif desc[:2] == b"T:":
            if dense_u: fail("table number for a dense user: %r" % f)
            if numB.get(utext) != desc[2:]: fail("table number %r is not the table's number for the text (%r): %r" % (desc, numB.get(utext), f))
        elif desc[:2] == b"L:":
            if dense_u: fail("local number for a dense user: %r" % f)
        else: fail("unknown user field: %r" % f)
        if not LIGHT and ukind.setdefault(utext, desc[:1]) != desc[:1]: fail("user ID with two kinds of code: %r" % f)
    rowsBl.add(tuple(f[:-2]) + (label(desc, nameB),))
diff("rows: reference vs dense", rowsA, rowsB)
del rowsA, rowsB, route, ukind

# 4. records == rows
recB = recs(B)
diff("dense sse2: rows vs records", rowsBl, recB)
del rowsBl

# 5. every other path
done = []
for isa in ("sse2", "avx2", "avx512z", "avx512y"):
    for t in (1, 3):
        if isa == "sse2" and t == 1: continue
        d, k = run("V-%s-%d" % (isa, t), dict(dense_env, SS_ISA=isa), t)
        if k != keys: fail("keys differ in %s/%d: %r vs %r" % (isa, t, k, keys))
        general = sum(1 for _ in lines(d, "rows"))
        diff("records: dense sse2 vs %s with %d threads" % (isa, t), recB, recs(d))
        done.append("%s/%d(%d general)" % (isa, t, general))

print("events %d (dense %d), clicks %d (dense %d), valid-line users %d (dense %d, complex index %d); dense on: events %d clicks %d; limits %d %d %d; runs: %s"
      % (nE, nEd, nC, nCd, nU, nUd, nUcomplex, ON_E, ON_C, LIME, LIMC, LIMI, " ".join(done)))
print("DENSE_PARSE_OK" if not bad else "DENSE_PARSE_FAIL (%d)" % bad)
sys.exit(1 if bad else 0)
