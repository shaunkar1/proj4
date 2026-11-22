#!/usr/bin/env python3
# usage: clwin_sets.py BASE OUT SEED [RATE=0.08]
# A copy of the data set BASE with damaged CLICK lines, for the click line readers (click_long_fields of the line
# code, the cl8g kernel): OUT/clicks/*.csv rewritten, everything else linked. Damage is aimed at the window cuts:
# IDs stretched to 30..70 bytes (a part that ends at byte 62, 63, 64, 65 of its window), quotes added / dropped /
# doubled / moved, commas and newlines put into or taken out of fields, long hex IDs with one odd character, upper
# case, 31 / 33 digits, fields emptied, a fifth field, CR before the newline, the last line without a newline.
import os, random, sys
base, out, seed = sys.argv[1], sys.argv[2], int(sys.argv[3]); rate = float(sys.argv[4]) if len(sys.argv) > 4 else 0.08
rnd = random.Random(seed)
os.makedirs(out + '/clicks', exist_ok=True)
for name in os.listdir(base):
    if name != 'clicks' and not os.path.exists(out + '/' + name): os.symlink(os.path.abspath(base + '/' + name), out + '/' + name)
HEX = b'0123456789abcdef'; B36 = b'0123456789abcdefghijklmnopqrstuvwxyz'
def rid(letter, n, alpha): return letter + bytes(rnd.choice(alpha) for _ in range(n))
def unq(f): return f[1:-1] if len(f) >= 2 and f[:1] == b'"' and f[-1:] == b'"' else f
def damage(line):
    f = line.split(b',')
    k = rnd.randrange(22)
    if len(f) != 4: return line
    i = rnd.randrange(4)
    if k == 0: f[i] = b'"' + unq(f[i]) + b'"'
    elif k == 1: f[i] = unq(f[i])
    elif k == 2: f[i] = b'"' + f[i]
    elif k == 3: f[i] = f[i] + b'"'
    elif k == 4:                                        # a quote inside
        j = rnd.randrange(len(f[i]) + 1); f[i] = f[i][:j] + b'"' + f[i][j:]
    elif k == 5:                                        # stretch an ID so that its part ends near a window cut
        i = rnd.randrange(2); n = rnd.choice([28, 30, 31, 32, 33, 34, 40, 58, 59, 60, 61, 62, 63, 64, 65, 66, 70, 100, 130])
        f[i] = rid(b'ku'[i:i+1], n, B36 if rnd.random() < 0.5 else HEX)
        if rnd.random() < 0.4: f[i] = b'"' + f[i] + b'"'
    elif k == 6:                                        # long hex ID, maybe with one odd character
        i = rnd.randrange(2); t = bytearray(rid(b'ku'[i:i+1], 32, HEX))
        r = rnd.random()
        if r < 0.3: t[rnd.randrange(1, 33)] = rnd.choice(b'gzAF /`@:G')
        elif r < 0.4: t[0] = rnd.choice(b'KUe0')
        elif r < 0.5: t = t[:rnd.choice([31, 32, 34])] if rnd.random() < 0.7 else t + b'0'
        f[i] = bytes(t)
        if rnd.random() < 0.3: f[i] = b'"' + f[i] + b'"'
    elif k == 7: f[i] = b''
    elif k == 8: f[i] = b'""'
    elif k == 9: f.append(rnd.choice([b'', b'x', b'"x"']))
    elif k == 10: f = f[:3]
    elif k == 11:                                       # a comma inside a (maybe quoted) field
        j = rnd.randrange(len(f[i]) + 1); f[i] = f[i][:j] + b',' + f[i][j:]
    elif k == 12: f[3] = f[3] + b'\r'
    elif k == 13:                                       # a newline inside the line
        s = b','.join(f); j = rnd.randrange(len(s) + 1); return s[:j] + b'\n' + s[j:]
    elif k == 14: f[i] = f[i].upper()
    elif k == 15: f[2] = rnd.choice([b'c_', b'c_0', b'c_00001', b'C_1', b'c_99999', b'c_1 ', b' c_1', b'c1', b'"c_1"x', f[2] + b'0', f[2][:-1]])
    elif k == 16: f[3] = rnd.choice([b'2027-12-12T14:19:43z', b'2027-12-12 14:19:43Z', b'1828471838', b'18284718380000', b'2027-12-12T14:19:43+00:00', b'2027-12-12T24:19:43Z', b'2027-13-12T14:19:43Z', b'2027-12-12T14:19:43-05:30', b'"2027-12-12T14:19:43+05:30"', b'2027-12-12T14:19:43.000Z', b''])
    elif k == 17:                                       # both IDs long and quoted: the longest ordinary line
        f[0] = b'"' + rid(b'k', 32, HEX) + b'"'; f[1] = b'"' + rid(b'u', 32, HEX) + b'"'
        if rnd.random() < 0.5: f[2] = b'"' + unq(f[2]) + b'"'
        if rnd.random() < 0.5: f[3] = b'"' + unq(f[3]) + b'"'
    elif k == 18:                                       # pad a field with blanks to move the cuts
        f[i] = f[i] + b' ' * rnd.choice([1, 20, 40, 60])
    elif k == 19:                                       # a random byte replaced
        s = bytearray(b','.join(f)); s[rnd.randrange(len(s))] = rnd.choice(b'",\n\r\x00 \x80az09'); return bytes(s)
    elif k == 20:                                       # a byte dropped
        s = b','.join(f); j = rnd.randrange(len(s)); return s[:j] + s[j+1:]
    else:                                               # short IDs of every length 1..12
        i = rnd.randrange(2); f[i] = rid(b'ku'[i:i+1], rnd.randrange(0, 12), B36)
    return b','.join(f)
# Only lines whose click_id occurs once in the whole set are damaged, and a damaged line whose click_id (read as the
# pipeline reads it) then equals that of any other line is put back: so no two lines of one click_id differ and the
# answer is the same for every reading order and thread count.
def ids_of(chunk):                                       # the click_id of each physical line of chunk (None: not four well-formed fields)
    r = []
    for ln in chunk.split(b'\n'):
        f = ln.split(b','); ok = len(f) == 4 and len(ln) > 0
        g = []
        for x in f:
            if x[:1] == b'"':
                if len(x) < 2 or x[-1:] != b'"' or b'"' in x[1:-1]: ok = False
                g.append(x[1:-1])
            else:
                if b'"' in x: ok = False
                g.append(x)
        r.append(g[0] if ok else None)
    return r
files = sorted(os.listdir(base + '/clicks')); texts = {}; count = {}
for name in files:
    data = open(base + '/clicks/' + name, 'rb').read()
    lines = data.split(b'\n'); tail = lines.pop() if lines else b''
    texts[name] = (lines, tail)
    for ln in lines[1:]:
        for k in ids_of(ln):
            if k is not None: count[k] = count.get(k, 0) + 1
out_lines = {}; damaged = []
for name in files:
    lines, tail = texts[name]; o = list(lines)
    for i in range(1, len(lines)):
        k = ids_of(lines[i])[0]
        if k is not None and count.get(k, 0) == 1 and rnd.random() < rate:
            o[i] = damage(lines[i]); damaged.append((name, i))
    out_lines[name] = o
newcount = {}
for name in files:
    for ln in out_lines[name][1:]:
        for k in ids_of(ln):
            if k is not None: newcount[k] = newcount.get(k, 0) + 1
back = 0
for name, i in damaged:
    if any(k is not None and newcount.get(k, 0) > 1 for k in ids_of(out_lines[name][i])):
        out_lines[name][i] = texts[name][0][i]; back += 1
for name in files:
    o = out_lines[name]; tail = texts[name][1]
    body = b'\n'.join(o) + b'\n' + tail
    if rnd.random() < 0.5 and body.endswith(b'\n'): body = body[:-1]          # the last line without a newline
    open(out + '/clicks/' + name, 'wb').write(body)
print('damaged lines', len(damaged) - back, 'put back', back)
