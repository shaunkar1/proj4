# The dense route

Work in progress on branch `dense`. `main` stays the scored line until this is verified.

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

* User field of a record (`PRec.user`, `CRec.user`, `DRec.user`, `DCRec.user`):
  * below `DENSE_USER` (2^30): ID table number, as today (the table limit must stay below 2^30, else dense is off);
  * `DENSE_USER | (index<<3 | alias)`: dense user ID with alias 0-7 whose index is not complex;
  * `LOCAL_USER | n`: thread-local fallback, as today.
* Person numbers: `[0, BASE)` are table numbers as today, `BASE` = the ID table's limit;
  `BASE + (index<<3 | root)` is the person of a non-complex index, `root` = the smallest alias in the
  component of the ID's alias under that index's simple links. `npersons = BASE + (limI<<3)`.
* A dense person's name is the byte-wise smallest text among the IDs of its component
  (`dn::text36('u', dn::value_of(index<<9 | alias, dense.ku), ...)`), including aliases that only
  appear in link lines.

## Stages

1. **KEYS** (done, in `run()`): sets `dense`, allocates `denseComplex`.
2. **LINKS** (agent B). Pass 1 over the link lines: a line is *simple* if both IDs are dense user IDs
   (index < limI) with alias 0-7 and the same index; for every other line set the complex bit of each
   dense-decodable index on it (any alias). Pass 2: a simple line whose index is not complex is a
   union inside that index's 8-alias component word; every other line goes through the ID table and
   union-find as today.
3. **PARSE** (agent A).
   * User of an event or click line: dense-decodable with alias 0-7 and `!dense_complex(index)` ->
     `DENSE_USER | (index<<3|alias)`, no table access. Anything else -> the table as today.
   * Event line with a dense event_id -> `DRec` into `tl.db[ve >> shift]`, valid or not (`k = 0` when
     not valid, exactly as `PRec`), `ingest` = exact milliseconds. Other event lines -> `PRec` as today.
   * Valid click line with a dense click_id -> `DCRec` into `tl.dcb[kc >> shift]`. Others -> `CRec`.
   * The general line handlers (`on_event`, `on_click`) classify the same way.
4. **DEDUP** of the general buckets (agent B): as today; a `DENSE_USER` user becomes its person number
   and its index is marked as having rows in the general lane.
5. **DENSE1** (agent B): per event bucket, direct-index dedup by counter (smallest ingest wins, same tie
   rule as the general dedup) into one global array indexed by event counter; per user index the first
   and last counter and the number of surviving events. Winners whose user is not `DENSE_USER` go to the
   general per-person lists. The same for clicks (copies of a click are identical).
6. **DENSE2** (agent B): per user index. *Fast* if the index has no rows in the general lane, its
   counter ranges are tight (`last-first+1 <= 4*count+64`) and all its rows belong to one person:
   gather its events and clicks from the two arrays, sort, sessionize with exactly the general
   semantics, write rows and totals. Otherwise append its events and clicks to the general lists.
7. **AGGREGATE** of the general lists (agent B): as today, with names for dense persons.

## Switches

`SS_NODENSE=1` (general route only), `SS_DSHIFT` / `SS_CSHIFT` (log2 of counters per event / click
bucket, default 14), plus whatever each stage needs for measuring.
