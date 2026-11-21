#!/usr/bin/env python3
"""Adversarial copy of a generated data set for the parse stage of the dense route (used with dense_check.py).
Extends the ideas of the coordinator's mutate.py with quoted fields, single odd bytes, values around 2^40,
counters around the limits, wrong letters and IDs made with another column's key.

usage: dense_mut.py BASE OUT SEED KE KU KC LIME LIMC LIMI     (numbers from the 'STATS dense keys' line)
env DM_BIN=1 also puts NUL and bytes >= 0x80 into IDs (the text dumps cope; compare.py does not).

Every change is decided per event_id / click_id and applied to all lines with that ID, and a forged ID is
never one that exists already, so copies stay copies and no two different rows share an ID."""
import os, random, shutil, sys
base, out, seed = sys.argv[1], sys.argv[2], int(sys.argv[3])
KE, KU, KC, LIME, LIMC, LIMI = map(int, sys.argv[4:10])
BIN = os.environ.get("DM_BIN") == "1"
R = random.Random(seed)
MASK = (1 << 40) - 1; M1 = 0x9E3779B97F4B & MASK; M2 = 0xD6E8FEB86659 & MASK
D = "0123456789abcdefghijklmnopqrstuvwxyz"
def b36(v):
    s = ""
    while True:
        s = D[v % 36] + s; v //= 36
        if v == 0: return s
def value_of(i, k):
    m = ((i * M1) ^ k) & MASK; m ^= m >> 17; return (m * M2) & MASK
def unq(f): return f[1:-1] if len(f) >= 2 and f[0] == '"' and f[-1] == '"' else f
def load(sub):
    files = {}
    for fn in sorted(os.listdir(os.path.join(base, sub))):
        with open(os.path.join(base, sub, fn), newline="", encoding="latin-1") as f: text = f.read()
        lines = text.split("\n")
        if lines and lines[-1] == "": lines.pop()
        files[fn] = lines
    return files
ODD = ["/", ":", "`", "{", "@", "[", "A", "Z", " ", "-", "_", "g".upper()] + (["\x00", "\x80", "\xff", "\xe9"] if BIN else [])
def odd_spelling(i):
    """A text that is not the canonical ID i (and not canonical at all, or canonical with another value)."""
    if not i: return R.choice(["u", "e", "k", "0"])
    L, body = i[0], i[1:]
    c = R.randrange(14)
    if c == 0: return i.upper()
    if c == 1: return L + "0" + body                       # leading zero
    if c == 2: return L + "00" + body
    if c == 3:                                             # wrong letter: any other one, or one bit away
        n = R.choice("xEUKeuk0z") if R.random() < 0.4 else chr(ord(L) ^ (1 << R.randrange(8 if BIN else 7)))
        return (n if n != L and n not in ',"\n\r' else "x") + body
    if c == 4: return i + R.choice(["z", "zz", "0", "00000"])     # longer (9+ characters with most IDs)
    if c == 5: return L + body.capitalize()
    if c == 6: return L + "z" * R.randrange(8, 12)                # 36^8-1 and longer
    if c == 7 and body: p = R.randrange(len(body)); return L + body[:p] + R.choice(ODD) + body[p + 1:]   # one odd byte
    if c == 8 and body:
        p = R.randrange(len(body)); d = D.find(body[p])
        # upper case, or the byte '0'+digit value (':' for a ... 'S' for z): what a careless decoder reads as the same digit
        return L + body[:p] + (body[p].upper() if d < 10 or R.random() < 0.3 else chr(0x30 + d)) + body[p + 1:]
    if c == 9: return L + b36(R.choice([MASK - 1, MASK, MASK + 1, MASK + 2, 36 ** 8 - 1, 36 ** 8, R.randrange(MASK + 1, 36 ** 8)]))
    if c == 10: return L + "0" * R.randrange(1, 10)               # zero in every length
    if c == 11: return R.choice([L, "", L + L, body, L + " " + body, " " + i, i + " "])
    if c == 12: return L + ("0" + body)[:8]                       # eight characters with a leading zero
    return L + b36(R.randrange(0, 36 ** R.randrange(1, 9)))       # a canonical text of any length (counter anywhere)
def forged(letter, key, lim):
    c = R.randrange(5)
    if c == 4:                                             # a counter below the limit whose text value is 2^40 or 2^41 too large
        v = value_of(R.randrange(0, max(1, lim)), key) + (MASK + 1) * R.randrange(1, 3)
        return letter + b36(v)                             # 8 characters if below 36^8, else 9
    if c == 0: return letter + b36(value_of(R.choice([lim - 2, lim - 1, lim, lim + 1, lim + 2]), key))
    if c == 1: return letter + b36(value_of(R.choice([0, 1, MASK, MASK - 1, R.randrange(lim, MASK)]), key))
    if c == 2: return letter + b36(value_of(R.randrange(0, lim), R.choice([KE, KU, KC])))   # perhaps another column's key
    return letter + b36(value_of(R.randrange(0, max(1, lim // 2)) * 2 + 1, key))
def forged_user():
    c = R.randrange(7)
    if c == 6: return "u" + b36(value_of(R.randrange(0, 4000) * 512 + R.randrange(8), KU) + (MASK + 1) * R.randrange(1, 3))
    if c == 0: return "u" + b36(value_of(R.choice([LIMI - 1, LIMI, LIMI + 1]) * 512 + R.choice([0, 7, 8, 511]), KU))
    if c == 1: return "u" + b36(value_of(R.randrange(0, 4000) * 512 + R.choice([7, 8, 9, 15, 16, 255, 256, 511]), KU))
    if c == 2: return "u" + b36(value_of(R.randrange(0, 4000) * 512 + R.randrange(8), KU))
    if c == 3: return "u" + b36(value_of(R.randrange(0, 1 << 22), R.choice([KE, KC])))
    if c == 4: return R.choice(["e", "k"]) + b36(value_of(R.randrange(0, 4000) * 512, KU))
    return "u" + b36(R.choice([MASK - 1, MASK, MASK + 1, 0, 35, 36]))
def q(x): return '"' + x + '"'
ev, cl = load("events"), load("clicks")
def groups(tab, ncomma):
    g = {}
    for fn, ls in tab.items():
        for n in range(1, len(ls)):
            if ls[n].count(",") == ncomma: g.setdefault(unq(ls[n].split(",")[0]), []).append((fn, n))
    return g
def mutate(tab, ncomma, letter, key, lim, share):
    g = groups(tab, ncomma); used = set(g); changed = 0
    for i, rows in list(g.items()):
        x = R.random()
        if x > share: continue
        f = [unq(v) for v in tab[rows[0][0]][rows[0][1]].split(",")]
        c = R.randrange(10)
        if c <= 2:
            n = odd_spelling(f[0]) if c < 2 else forged(letter, key, lim)
            if n in used or "," in n or '"' in n or "\n" in n: continue
            used.add(n); f[0] = n
        elif c <= 4:
            f[1] = odd_spelling(f[1]) if c == 3 else forged_user()
            if "," in f[1] or '"' in f[1]: continue
        elif c == 5: f[1] = letter + f[1][1:]                      # the user ID with this column's letter
        elif c == 6: f[0], f[1] = f[1], f[0]                       # swapped columns ...
        if c == 6 and f[0] in used: continue                       # ... unless that makes two rows share an ID
        if c == 6: used.add(f[0])
        w = list(f)
        if c == 7: w[0] = q(w[0])                                  # quoted fields: same values
        elif c == 8: w[1] = q(w[1])
        elif c == 9: w = [q(v) for v in w]
        if R.random() < 0.15: w[0] = q(w[0])
        if R.random() < 0.15: w[1] = q(w[1])
        for fn, n in rows: tab[fn][n] = ",".join(w)
        changed += len(rows)
    return changed
ne = mutate(ev, 6, "e", KE, LIME, 0.12)
nc = mutate(cl, 3, "k", KC, LIMC, 0.12)
for sub, tab in (("events", ev), ("clicks", cl)):
    os.makedirs(os.path.join(out, sub), exist_ok=True)
    for fn, lines in tab.items():
        head, body = lines[0], lines[1:]
        R.shuffle(body)
        with open(os.path.join(out, sub, fn), "w", newline="", encoding="latin-1") as f: f.write("\n".join([head] + body) + "\n")
shutil.copytree(os.path.join(base, "links"), os.path.join(out, "links"))
for fn in ("products.csv", "campaigns.csv", "manifest.json"): shutil.copy(os.path.join(base, fn), os.path.join(out, fn))
print("dense_mut: %d event lines and %d click lines changed" % (ne, nc))
