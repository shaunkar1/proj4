# Shopping Sessions — C++ competition pipeline

Independent implementation of the [Databricks / Builders Buildathon Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions) (scorer `shopping-sessions-4`).

## Design

1. Parse the challenge's precise CSV dialect (no whitespace normalization; `\r` preserved).
2. Build transitive connected components for linked user IDs, canonicalized to the byte-wise minimum.
3. Keep the earliest ingested occurrence of each validly parsed event ID **before** validating event content. Validate timestamps, types, prices, windows, and late events.
4. Deduplicate and validate clicks, merge users, then sort events and clicks by canonical person and timestamp.
5. Compute sessions from 30-minute timestamp gaps. For every session, compute the last eligible click and maintain a sliding seven-day set of distinct assisting campaigns.
6. Aggregate user/day and campaign/day. Emit hex-safe intermediary records to `/scratch` and write precisely typed Parquet with DuckDB.

Build is self-contained (only a DuckDB wheel at Docker build time), and the runtime has no network requirements. Core memory usage scales with unique event IDs; it is designed for the track's 24 GB memory limit.

## Build and run

```bash
docker build --platform linux/amd64 -t shopping-sessions .
docker run --rm --platform linux/amd64 -v "$PWD/dev-data:/data:ro" -v "$PWD/output:/output" -v "$PWD/scratch:/scratch" shopping-sessions
```

`/output/user_daily/part-00000.parquet` and `/output/campaign_daily/part-00000.parquet` are generated. The evaluator controls `DATA_DIR`, `OUTPUT_DIR`, and `SCRATCH_DIR`.

## Verification

Run `python -m unittest discover -s tests -v` locally to exercise the core with synthetic edge cases (requires g++). **This is not a replacement for the organizer's official 36 fixtures or `make check-dev`.** Before submitting, extract the official `starter-duckdb.zip`, `generator.zip`, and `dev-answer.zip`, generate the one-million-event sample, and run its comparison script against both Parquet outputs.

Do not add datasets, generated outputs, or starter ZIPs to the public repository.

## Performance notes

The current core is single-threaded for parsing and sorting, and stores an event hash table during deduplication. Its expected runtime and compliance with all hidden edge cases are **unverified**. Profile on the real 1M dev set and then tune parallel input parsing, dedup memory, and sort if needed. Correctness must precede optimization because incorrect rows receive no score.