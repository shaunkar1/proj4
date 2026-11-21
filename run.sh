#!/bin/sh
set -eu
: "${DATA_DIR:=/data}"
: "${OUTPUT_DIR:=/output}"
: "${SCRATCH_DIR:=/scratch}"
export OMP_NUM_THREADS="${NCPU:-8}"
mkdir -p "$OUTPUT_DIR/user_daily" "$OUTPUT_DIR/campaign_daily" "$SCRATCH_DIR"
/app/shopping-core
exec python /app/write_parquet.py