"""Write the challenge's exact two Parquet schemas from C++ intermediates."""
import os
from pathlib import Path
import duckdb

scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
output = Path(os.environ.get("OUTPUT_DIR", "/output"))
(output / "user_daily").mkdir(parents=True, exist_ok=True)
(output / "campaign_daily").mkdir(parents=True, exist_ok=True)
conn = duckdb.connect(
    database=":memory:",
    config={"threads": str(int(os.getenv("NCPU", "8"))),
            "memory_limit": "1GB", "temp_directory": str(scratch)},
)

def sql_path(path):
    return "'" + str(path).replace("'", "''") + "'"

def copy_parquet(query, path):
    conn.execute(f"COPY ({query}) TO {sql_path(path)} (FORMAT PARQUET, COMPRESSION SNAPPY)")

user_file = scratch / "user_rows.csv"
camp_file = scratch / "campaign_rows.csv"

if user_file.stat().st_size:
    user_query = f"""
    SELECT decode(unhex(user_hex)) AS user_id,
           (DATE '1970-01-01' + day_num) AS day,
           sessions, events, revenue_cents,
           CASE WHEN category_hex='NULL' THEN NULL
                ELSE decode(unhex(category_hex)) END AS top_category
    FROM read_csv({sql_path(user_file)}, header=false, delim=',', quote='',
       escape='', nullstr='__NEVER_NULL__',
       columns={{'user_hex':'VARCHAR','day_num':'INTEGER','sessions':'BIGINT',
                'events':'BIGINT','revenue_cents':'BIGINT','category_hex':'VARCHAR'}})
    """
else:
    user_query = """
    SELECT CAST(NULL AS VARCHAR) AS user_id, CAST(NULL AS DATE) AS day,
           CAST(NULL AS BIGINT) AS sessions, CAST(NULL AS BIGINT) AS events,
           CAST(NULL AS BIGINT) AS revenue_cents,
           CAST(NULL AS VARCHAR) AS top_category
    WHERE FALSE
    """
copy_parquet(user_query, output / "user_daily" / "part-00000.parquet")

if camp_file.stat().st_size:
    camp_query = f"""
    SELECT decode(unhex(campaign_hex)) AS campaign_id,
           decode(unhex(channel_hex)) AS channel,
           (DATE '1970-01-01' + day_num) AS day,
           sessions, purchasing_sessions, revenue_cents, assisted_sessions
    FROM read_csv({sql_path(camp_file)}, header=false, delim=',', quote='',
       escape='', nullstr='__NEVER_NULL__',
       columns={{'campaign_hex':'VARCHAR','channel_hex':'VARCHAR','day_num':'INTEGER',
                'sessions':'BIGINT','purchasing_sessions':'BIGINT','revenue_cents':'BIGINT',
                'assisted_sessions':'BIGINT'}})
    """
else:
    camp_query = """
    SELECT CAST(NULL AS VARCHAR) AS campaign_id,
           CAST(NULL AS VARCHAR) AS channel,
           CAST(NULL AS DATE) AS day, CAST(NULL AS BIGINT) AS sessions,
           CAST(NULL AS BIGINT) AS purchasing_sessions,
           CAST(NULL AS BIGINT) AS revenue_cents,
           CAST(NULL AS BIGINT) AS assisted_sessions
    WHERE FALSE
    """
copy_parquet(camp_query, output / "campaign_daily" / "part-00000.parquet")