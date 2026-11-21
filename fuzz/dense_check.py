#!/usr/bin/env python3
"""Checks the parse stage of the dense route (DENSE.md stage 3) with the SS_DENSE_DUMP text dumps.

usage: dense_check.py CORE DATA_DIR WORK_DIR [K=V ...]      (K=V: extra environment for the dense runs,
                                                             e.g. SS_TEST_LIME=5000 SS_TEST_COMPLEX=3)
Runs CORE on DATA_DIR
  A  with SS_NODENSE=1, SS_ISA=sse2, one thread: the reference. Its rows-*.txt hold every event / click line
     that produces a record, with the raw ID texts, as the general handlers saw it.
  R  with SS_DENSE_RECORDS=1 on every ISA with 1, 2, 3 and 8 threads (DC_PLAN=isa:threads,... picks other runs).
From A and an independent decoder of the ID texts (below) the script works out what the parse stage must
leave behind, and compares every run R with it:
  1. event slots: exactly the dense event IDs of A are in use; each slot holds the copy with the smallest
     ingest_ts (ingest, k, category, user), or "not a valid event" if that copy is not valid; no slot is busy
  2. click slots: exactly the dense click IDs of A's valid clicks, with key and user of the click
  3. bucket records (general route): the multiset of all other rows of A
  4. users: a dense user code regenerates the user ID text, and exactly the texts the decoder calls dense
     (canonical, alias < 8, index below the limit and not complex) have one; a table number is the number
     the run's ID table has for the text
An ID takes the dense route iff it is letter + canonical base 36 with a counter below the limit.
IDs whose copies leave the result undefined (equal smallest ingest_ts but different content; clicks that
differ) are counted and skipped.
With SS_TEST_MAXNEW (full ID table) the users that are not dense are only checked for "not dense":
which of them still got a table number depends on the order the threads met them.
DC_FALLBACK=1 asks for the same without SS_TEST_MAXNEW: needed on sets with so many forged user IDs that the
ID table of a dense run (an eighth of the room for new IDs) fills up by itself, e.g. the dense_run.sh sets.
Every dense run writes its dump into WORK_DIR/dump and is read before the next run overwrites it;
A's dump stays in WORK_DIR/dumpA. WORK_DIR may be reused. Prints DENSE_PARSE_OK or the first differences."""
import os, subprocess, sys
from collections import Counter

core, data, work = sys.argv[1], sys.argv[2], sys.argv[3]
extra = dict(a.split("=", 1) for a in sys.argv[4:])
FALLBACK = "SS_TEST_MAXNEW" in extra or os.environ.get("DC_FALLBACK") == "1"
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

def run(name, env, threads, sub="dump"):
    out = os.path.join(work, sub)
    e = dict(os.environ); e.update(env)
    for k in ("SS_NODENSE", "SS_DENSE_USERS", "SS_DENSE_RECORDS", "SS_ISA"):
        if k not in env: e.pop(k, None)
    o = os.path.join(work, "out"); os.makedirs(o, exist_ok=True)
    e.update(DATA_DIR=data, OUTPUT_DIR=o, SCRATCH_DIR=o, NCPU=str(threads), SS_DENSE_DUMP=out, SS_STATS="1")
    p = subprocess.run([core], env=e, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if p.returncode != 0: sys.exit("run %s failed with code %d" % (name, p.returncode))
    keys = None; on = [0, 0]; sw = [0, 0, 0, 0]
    for line in p.stderr.decode("latin-1").split("\n"):
        if line.startswith("STATS dense keys"):
            w = line.split(); keys = [int(x) for x in w[3:6] + w[7:10]] + on
        elif line.startswith("dense: events"):       # whether the keys were found at all (printed before the STATS line)
            w = line.split(); on = [int(w[2]), int(w[4])]
        elif line.startswith("dense parse: users"):  # what the parse stage really does (printed after the links stage)
            w = line.split(); sw = [int(w[3]), int(w[6]), int(w[9]), int(w[11]) if len(w) > 11 else 0]
    if keys: keys = keys + sw
    return out, keys

def lines(d, kind, threads):
    for t in range(threads):     # only this run's files: an earlier run with more threads may have left others
        with open(os.path.join(d, "%s-%d.txt" % (kind, t)), "rb") as f:
            for ln in f: yield ln.rstrip(b"\n").split(b"\t")

def users(d):
    num = {}
    with open(os.path.join(d, "users.txt"), "rb") as f:
        for ln in f:
            n, t = ln.split()
            num[t] = n
    return num

def diff(what, a, b):
    if a == b: return
    only_a, only_b = a - b, b - a
    fail("%s: %d lines only in the first, %d only in the second" % (what, sum(only_a.values()), sum(only_b.values())))
    for x in list(only_a)[:3]: print("   first :", x)
    for x in list(only_b)[:3]: print("   second:", x)

dense_env = dict(extra); dense_env.setdefault("SS_DENSE_RECORDS", "slot")   # or bucket, from the command line
A, _ = run("A-nodense-sse2", {"SS_NODENSE": "1", "SS_ISA": "sse2"}, 1, "dumpA")
plan = [("sse2", 1), ("sse2", 3), ("avx2", 1), ("avx2", 2), ("avx2", 3), ("avx2", 8), ("avx512z", 1), ("avx512z", 3), ("avx512y", 1), ("avx512y", 3), ("sse2", 8)]
if os.environ.get("DC_PLAN"):   # e.g. DC_PLAN=avx2:8,avx2:8,sse2:2
    plan = [(a.split(":")[0], int(a.split(":")[1])) for a in os.environ["DC_PLAN"].split(",")]
keys = None; done = []
for isa, threads in plan:
    env = dict(dense_env, SS_ISA=isa)
    name = "%s/%d" % (isa, threads)
    R, k = run(name, env, threads)
    if not k: sys.exit("no 'STATS dense keys' line")
    if keys is None:
        # ---- what the parse stage must leave behind, from the reference rows ----
        keys = k
        KE, KU, KC, LIME, LIMC, LIMI, ON_E, ON_C, SW_U, SW_E, SW_C, BUCKET = keys
        if not (ON_E and SW_E): LIME = 0   # keys not found for a column, or its dense route not in use
        if not (ON_C and SW_C): LIMC = 0
        if not SW_U: LIMI = 0
        exp_ev, exp_cl = {}, {}            # dense ID -> [ingest, k, cat, user text, undefined?] / [key, user text, undefined?]
        gen_rows = []                      # rows of the general route: (E, id, ingest, k, cat, user text) / (C, id, key, user text)
        den_rows = []                      # bucket form: every row with a dense ID, same shape
        nE = nC = 0
        for f in lines(A, "rows", 1):
            if f[1] != b"P" or f[-1][:2] == b"U:": fail("dense route in the SS_NODENSE run: %r" % f)
            idt = bytes.fromhex(f[2].decode())
            if f[0] == b"E":
                nE += 1
                c = counter(idt, 0x65, KE)
                row = [int(f[3]), f[4], f[5], f[6], False]
                if c is not None and c < LIME:
                    if BUCKET: den_rows.append((b"E", f[2], f[3], f[4], f[5], f[6]))
                    e = exp_ev.get(f[2])
                    if e is None or row[0] < e[0]: exp_ev[f[2]] = row     # (a smaller copy also clears an earlier tie)
                    elif row[0] == e[0] and row[1:4] != e[1:4]: e[4] = True
                else: gen_rows.append((b"E", f[2], f[3], f[4], f[5], f[6]))
            else:
                nC += 1
                c = counter(idt, 0x6B, KC)
                if c is not None and c < LIMC:
                    if BUCKET: den_rows.append((b"C", f[2], f[3], f[4]))
                    e = exp_cl.get(f[2])
                    if e is None: exp_cl[f[2]] = [f[3], f[4], False]
                    elif e[0] != f[3] or e[1] != f[4]: e[2] = True
                else: gen_rows.append((b"C", f[2], f[3], f[4]))
        undefE = sum(1 for e in exp_ev.values() if e[4]); undefC = sum(1 for e in exp_cl.values() if e[2])
    elif k != keys: fail("keys or switches differ in %s: %r vs %r" % (name, k, keys))
    num = users(R)
    with open(os.path.join(R, "complex.txt")) as f: complex_idx = set(int(x) for x in f.read().split())
    ucache = {}
    def user_label(utext):          # what a record's user field must show for this user ID text
        r = ucache.get(utext)
        if r is None:
            c = counter(bytes.fromhex(utext.decode()), 0x75, KU)
            if c is not None and (c & 511) < 8 and (c >> 9) < LIMI and (c >> 9) not in complex_idx: r = b"U:" + utext
            else: r = b"TL" if FALLBACK else b"T:" + num.get(utext, b"?")
            ucache[utext] = r
        return r
    def shown(desc):
        if FALLBACK and desc[:2] in (b"T:", b"L:"): return b"TL"
        return desc
    # 1./2. the slots
    seenE = seenC = 0
    with open(os.path.join(R, "slots.txt"), "rb") as fh:
        for ln in fh:
            f = ln.rstrip(b"\n").split(b"\t")
            if f[1] != b"D": fail("%s: slot left busy: %r" % (name, f)); continue
            if f[0] == b"E":      # E D id ingest k cat user thread
                seenE += 1; e = exp_ev.get(f[2])
                if e is None: fail("%s: slot in use for an ID without a dense row: %r" % (name, f)); continue
                if e[4]: continue
                if int(f[3]) != e[0] or f[4] != e[1]: fail("%s: slot %r, expected ingest %d k %s" % (name, f, e[0], e[1].decode())); continue
                if e[1] != b"0" and (f[5] != e[2] or shown(f[6]) != user_label(e[3])):
                    fail("%s: slot %r, expected cat %s user %s" % (name, f, e[2].decode(), user_label(e[3]).decode()))
            else:                 # C D id key user thread
                seenC += 1; e = exp_cl.get(f[2])
                if e is None: fail("%s: click slot in use for an ID without a dense valid click: %r" % (name, f)); continue
                if e[2]: continue
                if f[3] != e[0] or shown(f[4]) != user_label(e[1]): fail("%s: click slot %r, expected key %s user %s" % (name, f, e[0].decode(), user_label(e[1]).decode()))
    if not BUCKET:
        if seenE != len(exp_ev): fail("%s: %d event slots in use, expected %d" % (name, seenE, len(exp_ev)))
        if seenC != len(exp_cl): fail("%s: %d click slots in use, expected %d" % (name, seenC, len(exp_cl)))
    elif seenE or seenC: fail("%s: slots written in bucket form" % name)
    # 3. the bucket records
    want = Counter()
    for g in gen_rows:
        if g[0] == b"E": want[(b"E", g[1], g[2], g[3], g[4], user_label(g[5]) if g[3] != b"0" else b"-")] += 1
        else: want[(b"C", g[1], g[2], user_label(g[3]))] += 1
    wantB = Counter()               # bucket form: every dense row must be there, each exactly once
    for g in den_rows:
        if g[0] == b"E": wantB[(b"E", g[1], g[2], g[3], g[4] if g[3] != b"0" else b"0", user_label(g[5]) if g[3] != b"0" else b"-")] += 1
        else: wantB[(b"C", g[1], g[2], user_label(g[3]))] += 1
    got = Counter(); gotB = Counter()
    for f in lines(R, "recs", threads):
        if f[1] == b"P": got[(f[0],) + tuple(f[2:-1]) + (shown(f[-1]),)] += 1
        elif f[1] == b"B": gotB[(f[0],) + tuple(f[2:-1]) + (shown(f[-1]),)] += 1
        else: fail("%s: unknown record %r" % (name, f))
    diff("%s: general bucket records, expected vs found" % name, want, got)
    diff("%s: dense bucket records, expected vs found" % name, wantB, gotB)
    done.append(name)

print("events %d rows (%d dense IDs, %d undefined), clicks %d rows (%d dense IDs, %d undefined), general-route rows %d; keys found: events %d clicks %d; in use: users %d events %d clicks %d bucket form %d; limits %d %d %d; runs: %s"
      % (nE, len(exp_ev), undefE, nC, len(exp_cl), undefC, len(gen_rows), ON_E, ON_C, SW_U, SW_E, SW_C, BUCKET, LIME, LIMC, LIMI, " ".join(done)))
print("DENSE_PARSE_OK" if not bad else "DENSE_PARSE_FAIL (%d)" % bad)
sys.exit(1 if bad else 0)
