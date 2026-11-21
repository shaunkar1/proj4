# The dense route

Work in progress on branch `dense`. `main` stays the scored line until this is verified.
Check: `/home/claude/bb/mut/densecheck.sh` style equivalence runs (adversarial copies of generated sets) plus the usual references.

## What the generator gives us

`dn::` in `main.cpp` has the details. Every event_id, click_id and user_id is a counter pushed through a
fixed 40-bit bijection with one key per kind (`dense.ke`, `dense.kc`, `dense.ku`, found from the data
in the KEYS stage). Measured on the dev set:

* 99.98% of event rows have a canonical dense event_id; counters are exactly 0..N-1.
* A user ID is `index*512 + alias`. 99.8% of rows have a canonical dense user_id; aliases 0-7 cover 99.1%.
* A user index's events are consecutive event counters (index never decreases as the counter grows,
  apart from 0.05% of rows), and 99.84% of neighbours are already in time order.
* A user index's clicks are consecutive click counters, in random time order.
* Every link line with two dense IDs joins two aliases of the same index. 0.85% of link lines have a
  non-dense side (shared devices, upper-case variants); the indexes they touch hold 1.9% of events.

## The rule that keeps it exact

An ID takes the dense route only if its text is canonical (`dn::parse36`) with the right letter and its
counter is below the limit (`dense.limE`, `dense.limC`, `dense.limI`). Text and counter are then
one-to-one, so equal counters mean equal IDs and a dense ID never equals a general ID. The route of a
row depends only on the ID text, so all copies of an event or click take the same route. Everything
else, and everything when the keys are not found or `SS_NODENSE=1` is set, is the general route that
`main` has today. **The output must be identical to `SS_NODENSE=1` on every data set.**

## Numbers

* User field of a record or slot (`PRec.user`, `CRec.user`, `ESlot.user`, `CSlot.user`):
  * below `DENSE_USER` (2^30): ID table number, as today (the table limit must stay below 2^30, else dense users are off);
  * `DENSE_USER | (index<<3 | alias)`: dense user ID with alias 0-7 whose index is not complex;
  * `LOCAL_USER | n`: thread-local fallback, as today.
* Person numbers: `[0, BASE)` are table and fallback numbers as today (`BASE` is known after the
  fallback merge); `BASE + (index<<3 | root)` is the person of a non-complex index, `root` = the smallest
  alias in the group of the ID's alias under that index's simple links. The number space ends at the
  largest index the parser reported, not at `limI` (which is a loose upper bound).
* A dense person's name is the byte-wise smallest text among the IDs of its group, including aliases
  that only appear in link lines.

## Stages

1. **KEYS** (in `run()`): sets `dense`, allocates `denseComplex`.
2. **LINKS** (agent B, done). Pass 1 over the link lines: a line is *simple* if both IDs are dense user
   IDs (index < limI) with alias 0-7 and the same index; for every other line the complex bit of each
   dense-decodable index on it is set. Pass 2: a simple line whose index is not complex is a union
   inside that index's 8-alias word; every other line goes through the ID table and union-find as today.
3. **PARSE** (agent A).
   * User of an event or click line: dense-decodable with alias 0-7 and `!dense_complex(index)` ->
     `DENSE_USER | (index<<3|alias)`, no table access. Anything else -> the table as today.
   * Event line with a dense event_id -> straight into `denseEv[ve]`, a 32-byte slot
     `{atomic k; user; cat; ingest}` of one global array indexed by counter. `k`: 0 empty, 1 busy,
     2 present but not a valid event, otherwise the valid `k`. A row takes the slot if it is empty or if
     its ingest_ts is smaller than the slot's (compare-and-swap on `k` is the lock), so the slot ends up
     holding the copy with the smallest ingest_ts: this is the whole dedup of dense events. The slot is
     prefetched when the counter is known and written a couple of rows later.
   * Valid click line with a dense click_id -> `denseCl[kc]` (`{key; user; present}`, 16 bytes).
   * Other event and click lines -> `PRec` / `CRec` buckets as today.
   * The general line handlers classify the same way.
   (Measured on the scoring machine: scattering records into buckets and a second direct-index pass costs
   about 86 ms at 20M and does not scale to 60M; writing the slots directly costs 60-68 ms and is linear.)
4. **DEDUP** of the general buckets (agent B): as today; a `DENSE_USER` user becomes its person number
   and its index is marked as having rows in the general lane.
5. **DENSE** reader (agent B), parallel over counter ranges and then user-index ranges: per user index
   the first and last counter and the number of surviving events and clicks; slots whose user is not
   `DENSE_USER` go to the general per-person lists. An index is *fast* if it has no rows in the general
   lane, its counter ranges are tight (`last-first+1 <= 4*count+64`) and all its rows belong to one
   person: its events and clicks are gathered from the two arrays, sorted, and sessionized with exactly
   the general semantics. Any other index's rows are appended to the general lists.
6. **AGGREGATE** of the general lists (agent B): as today, with names for dense persons.

## Switches

`SS_NODENSE=1` (general route only), `SS_DENSE_USERS`, `SS_DENSE_RECORDS` (while the pieces are being
built), `SS_DENSE_AHEAD` (rows between prefetching and writing a slot), `SS_TEST_LIME/LIMC/LIMI`
(shrink the limits), plus whatever each stage needs for measuring.
