# Shopping Sessions — C++ competition pipeline

Independent implementation of the [Databricks / Builders Buildathon Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions) (scorer `shopping-sessions-4`).

## Design

1. Parse the challenge's precise CSV dialect (no whitespace normalization; `\r` preserved).
2. Build transitive connected components for linked user IDs, canonicalized to the byte-wise minimum.
3. Keep the earliest ingested occurrence of each validly parsed event ID **before** validating event content. Validate timestamps, types, prices, windows, and late events.
4. Deduplicate and validate clicks, merge users, then sort events and clicks by canonical person and timestamp.
5. Compute sessions from 30-minute timestamp gaps. For every session, compute the last eligible click and maintain a sliding seven-day set of distinct assisting campaigns.
6. Aggregate user/day and campaign/day and write both Parquet tables directly from C++ (PLAIN encoding, uncompressed, one file per thread for `user_daily`).

Build is self-contained (g++ only) and the runtime image holds a single binary; there is no Python or DuckDB step. Core memory usage scales with unique event IDs; it is designed for the track's 24 GB memory limit.

## Build and run

```bash
docker build --platform linux/amd64 -t shopping-sessions .
docker run --rm --platform linux/amd64 -v "$PWD/dev-data:/data:ro" -v "$PWD/output:/output" -v "$PWD/scratch:/scratch" shopping-sessions
```

`/output/user_daily/part-*.parquet` and `/output/campaign_daily/part-00000.parquet` are generated. The evaluator controls `DATA_DIR`, `OUTPUT_DIR`, and `SCRATCH_DIR`.

## Verification

Run `python -m unittest discover -s tests -v` locally to exercise the core with synthetic edge cases (requires g++). **This is not a replacement for the organizer's official 36 fixtures or `make check-dev`.** Before submitting, extract the official `starter-duckdb.zip`, `generator.zip`, and `dev-answer.zip`, generate the one-million-event sample, and run its comparison script against both Parquet outputs.

Do not add datasets, generated outputs, or starter ZIPs to the public repository.

## Performance notes

Event and click files are split into 2 MB chunks and parsed on all cores. Each parsed line goes into a bucket chosen by the hash of its event or click ID, so duplicates always land in the same bucket and each bucket is deduplicated on its own with a small cache-friendly table. Linked IDs are resolved through a read-only table during parsing; IDs that no link mentions are numbered per thread and merged afterwards. Sorting uses the parallel sort. Sessionizing and attribution run on blocks of people in parallel, and each thread writes its own Parquet file.

Checked against the organizer's 36 fixtures and the published 1M-event dev answer (seed 1).
