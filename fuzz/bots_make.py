#!/usr/bin/env python3
"""Big-bot variant of a generated data set, for the reader stages (index pass / general lane).

usage: bots_make.py BASE OUT R KE KU KC NEV NCL [NBOT=5] [SEED=1]
  KE KU KC: the keys of BASE (the 'STATS dense keys' line of SS_STATS=1 prints "ke ku kc").
  NEV NCL:  counters at or above these are not BASE's own (use the 'STATS dense slots: event end .. click end ..' numbers).

The generator's user index is a rank: indexes 0..4 are the "bots" (about 1.1% of all events each), and a user
index's events (clicks) are consecutive event (click) counters. This tool keeps that shape and makes the bots R
times as big without making the rest bigger: every event and click row of a bot (user index < NBOT, alias < 512,
canonical user ID) is written R times, copy j with its own ID (events: the same time, so the rows are exactly as
much in time order as BASE's; env BM_EV_STEP_MS=n: +n ms * j, which puts many rows out of order because the
generator's bots have runs of rows of one second; clicks: +7 s * j; an unreadable time is kept as it is, so all R copies are the same invalid row). All IDs are made
again from new counters, which stay consecutive: new counter = number of rows (with their copies) before the old
one, + j. Copies of one event (same ID, other ingest_ts) get the same new IDs, so duplicates stay duplicates.
Rows whose ID is not a canonical dense one (the general lane's rows) are written once, unchanged. On top of that
bots 0..2 get three events and two clicks with an odd ID (upper-case letter), bot 3 three events only, bot 4
none: so there are marked bots with general events and clicks, one with events only and one unmarked. An odd
click gets the time of the bot's copy 0 of the same click and another campaign, so that a general click and a
dense click of one moment have to be ordered by their ID texts.
Lines that cannot be read as 7 (events) / 4 (clicks) plain fields are dropped, as are canonical IDs beyond
NEV / NCL. Links, campaigns, products and the manifest are copied."""
import os, random, shutil, sys
from datetime import datetime, timezone
base, out, R = sys.argv[1], sys.argv[2], int(sys.argv[3])
KE, KU, KC, NEV, NCL = map(int, sys.argv[4:9])
NBOT = int(sys.argv[9]) if len(sys.argv) > 9 else 5
RND = random.Random(int(sys.argv[10]) if len(sys.argv) > 10 else 1)
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
def unq(f): return f[1:-1] if len(f) >= 2 and f[0] == '"' and f[-1] == '"' else f
def counter(text, letter, key):
    if not text or text[0] != letter: return None
    y = parse36(text[1:])
    return None if y is None else index_of(y, key)
def ms_of(text):
    """epoch milliseconds of a time field in one of the generator's forms, or None"""
    t = unq(text)
    if t.isdigit() and len(t) == 13: return int(t)
    try:
        d = datetime.fromisoformat(t)
        if d.tzinfo is None: return None
        return int(round(d.timestamp() * 1000))
    except ValueError: return None
def bot_of(user):
    c = counter(unq(user), 'u', KU)
    return c >> 9 if c is not None and (c >> 9) < NBOT else None
def lines_of(sub):
    for fn in sorted(os.listdir(os.path.join(base, sub))):
        with open(os.path.join(base, sub, fn), newline="", encoding="latin-1") as f:
            head = f.readline()
            yield fn, head, None
            for line in f: yield fn, None, line.rstrip("\n")
def make(sub, nf, letter, key, lim, tscol, step_ms, n_odd):
    # pass 1: copies per old counter; the bot of a counter
    mult = [0] * lim; bot = {}
    rows = []
    dropped = 0
    for fn, head, line in lines_of(sub):
        if head is not None: rows.append((fn, head, None, None)); continue
        p = line.split(",")
        if len(p) != nf: dropped += 1; continue
        idt = unq(p[0]); c = counter(idt, letter, key)
        if c is not None and c >= lim: dropped += 1; continue
        b = bot_of(p[1])
        if c is not None:
            mult[c] = max(mult[c], R if b is not None else 1)
            if b is not None: bot.setdefault(c, b)
        rows.append((fn, None, p, c))
    newbase = [0] * (lim + 1)
    for c in range(lim): newbase[c + 1] = newbase[c] + mult[c]
    # odd rows: per bot, n_odd[bot] of its counters (copy number chosen at random)
    by_bot = {}
    for c, b in bot.items(): by_bot.setdefault(b, []).append(c)
    odd = {}
    for b, cs in by_bot.items():
        cs.sort()
        for c in RND.sample(cs, min(len(cs), n_odd(b))): odd[c] = RND.randrange(1, R) if R > 1 else 0
    os.makedirs(os.path.join(out, sub), exist_ok=True)
    files = {}; nout = 0; nodd = 0; lastcamp = {}
    for fn, head, p, c in rows:
        f = files.get(fn)
        if f is None: f = files[fn] = open(os.path.join(out, sub, fn), "w", newline="", encoding="latin-1")
        if head is not None: f.write(head); continue
        if c is None: f.write(",".join(p) + "\n"); nout += 1; continue      # a general-lane row: as it is
        m = mult[c]; t0 = ms_of(p[tscol]) if m > 1 else None
        b = bot.get(c)
        for j in range(m):
            q = list(p)
            q[0] = letter + b36(value_of(newbase[c] + j, key))
            if j and t0 is not None: q[tscol] = str(t0 + j * step_ms)
            if odd.get(c) == j and j:
                q[0] = q[0][0].upper() + q[0][1:]; nodd += 1
                if sub == "clicks":
                    if t0 is not None: q[tscol] = str(t0)                     # the moment of copy 0, which keeps a dense ID
                    if lastcamp.get(b) is not None: q[2] = lastcamp[b]
            f.write(",".join(q) + "\n"); nout += 1
        if sub == "clicks" and b is not None: lastcamp[b] = p[2]
    for f in files.values(): f.close()
    sizes = {b: sum(mult[c] for c in cs) for b, cs in sorted(by_bot.items())}
    print(f"{sub}: {nout} rows written, {dropped} dropped, {nodd} odd IDs, new counters {newbase[lim]}, bot rows {sizes}")
os.makedirs(out, exist_ok=True)
make("events", 7, 'e', KE, NEV, 2, int(os.environ.get("BM_EV_STEP_MS", "0")), lambda b: 3 if b <= 3 else 0)
make("clicks", 4, 'k', KC, NCL, 3, 7000, lambda b: 2 if b <= 2 else 0)
for name in os.listdir(base):
    s = os.path.join(base, name); d = os.path.join(out, name)
    if name in ("events", "clicks"): continue
    if os.path.isdir(s): shutil.copytree(s, d, dirs_exist_ok=True)
    else: shutil.copy(s, d)
