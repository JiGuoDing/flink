#!/usr/bin/env python3
import sqlite3
import pandas as pd

db = "nsys_report.sqlite"
conn = sqlite3.connect(db)

# 1. 先把 NVTX_EVENTS 里我们关心的区间捞出来
sql = """
SELECT
    CASE
        WHEN text LIKE 'embed:%'   THEN 'embed'
        WHEN text LIKE 'rotary:%'  THEN 'rotary'
        WHEN text LIKE 'rmsnorm:%' THEN 'rmsnorm'
        WHEN text LIKE 'attn:%'    THEN 'attn'
        WHEN text LIKE 'mlp:%'     THEN 'mlp'
        WHEN text LIKE 'lm_head:%' THEN 'lm_head'
    END AS op_group,
    (end - start) AS dur_ns
FROM NVTX_EVENTS
WHERE text LIKE 'embed:%'
   OR text LIKE 'rotary:%'
   OR text LIKE 'rmsnorm:%'
   OR text LIKE 'attn:%'
   OR text LIKE 'mlp:%'
   OR text LIKE 'lm_head:%';
"""

df = pd.read_sql(sql, conn)
conn.close()

# 2. 汇总
summary = (
    df.groupby("op_group")["dur_ns"]
      .agg(calls="count",
           total_ms=lambda x: x.sum() / 1e6,
           avg_ms=lambda x: x.mean() / 1e6)
      .sort_values("total_ms", ascending=False)
)

print(summary)
summary.to_csv("op_times.csv", float_format="%.3f")
print("\n已写入 op_times.csv")