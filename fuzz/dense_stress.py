#!/usr/bin/env python3
"""Collision stress for the dense slots (used with dense_check.py).
usage: dense_stress.py BASE OUT NFILES LINES [SEED [REPS]]
Writes NFILES small event files that all hold the same LINES events in the same order, so threads working
on different files reach the same slots at the same moments. Every copy of an event has its own ingest_ts
(never equal: a tie between copies that differ would be undefined), and many copies differ in user, type
or validity; which file holds the smallest ingest_ts changes from event to event. The click files hold the
same valid clicks, identical in every file. Links and the small tables are copied.
With REPS > 1 every file runs through its events REPS times (NFILES * REPS <= 3600 copies of each event, all
with different ingest_ts): few events and many repetitions keep all threads on the same few slots."""
import os, random, shutil, sys
base, out, nfiles, nlines = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
R = random.Random(int(sys.argv[5]) if len(sys.argv) > 5 else 1)
reps = int(sys.argv[6]) if len(sys.argv) > 6 else 1
assert nfiles * reps <= 3600
def rows(sub, ncomma, want):
    seen, res, head = set(), [], None
    for fn in sorted(os.listdir(os.path.join(base, sub))):
        with open(os.path.join(base, sub, fn), newline="", encoding="latin-1") as f:
            for n, line in enumerate(f.read().split("\n")):
                if n == 0: head = line; continue
                if line.count(",") != ncomma or '"' in line: continue
                f0 = line.split(",")
                if f0[0] in seen: continue
                if sub == "events" and (len(f0[3]) != 20 or f0[3][-1] != "Z" or not (f0[3][14:16] + f0[3][17:19]).isdigit()): continue
                seen.add(f0[0]); res.append(f0)
                if len(res) == want: return head, res
    return head, res
eh, ev = rows("events", 6, nlines)
ch, cl = rows("clicks", 3, max(1, nlines // 4))
pool = [f[1] for f in R.sample(ev, min(len(ev), 500))]
shift = [R.randrange(3600) for _ in ev]
os.makedirs(os.path.join(out, "events")); os.makedirs(os.path.join(out, "clicks"))
for fi in range(nfiles):
    Rf = random.Random(fi * 7919 + 13)
    with open(os.path.join(out, "events", "part-%05d.csv" % fi), "w", newline="", encoding="latin-1") as o:
        o.write(eh + "\n")
        for rp, n, f in [(rp, n, f) for rp in range(reps) for n, f in enumerate(ev)]:
            w = list(f); t = ((fi * reps + rp) * 37 + shift[n]) % 3600      # distinct for every copy of the event
            w[3] = f[3][:14] + "%02d:%02dZ" % (t // 60, t % 60)
            c = Rf.randrange(10)
            if c == 0: w[1] = Rf.choice(pool)
            elif c == 1: w[4] = Rf.choice(["view", "cart", "purchase", "nothing"])
            elif c == 2: w[2] = "not-a-time"
            elif c == 3: w[1] = "general-user-%d" % Rf.randrange(50)      # a table user (long ID: general handler)
            elif c == 4: w[1] = "g%d" % Rf.randrange(500)                 # a table user (short ID: pending batch)
            o.write(",".join(w) + "\n")
    with open(os.path.join(out, "clicks", "part-%05d.csv" % fi), "w", newline="", encoding="latin-1") as o:
        o.write(ch + "\n")
        for f in cl: o.write(",".join(f) + "\n")
shutil.copytree(os.path.join(base, "links"), os.path.join(out, "links"))
for fn in ("products.csv", "campaigns.csv", "manifest.json"): shutil.copy(os.path.join(base, fn), os.path.join(out, fn))
print("dense_stress: %d files x %d events, %d clicks each" % (nfiles, len(ev), len(cl)))
