"""
Quick check: How many Return records exist with PERIOD_END >= 202301
"""

import os
from dotenv import load_dotenv
import oracledb

env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

ACTIVE_PERIOD = 202301

print("Checking Return record counts...\n")

# Connect to Oracle
try:
    conn = oracledb.connect(
        user=os.getenv('ORACLE_USER'),
        password=os.getenv('ORACLE_PASSWORD'),
        host=os.getenv('ORACLE_HOST'),
        port=int(os.getenv('ORACLE_PORT')),
        sid=os.getenv('ORACLE_SID')
    )
    print("[OK] Connected to Oracle\n")
except Exception as e:
    print(f"[ERROR] {e}")
    exit(1)

cursor = conn.cursor()

# Total Returns
print("CO_WSR Return Counts:")
print("-" * 60)

queries = {
    "Total Returns (all)": "SELECT COUNT(*) FROM SCH_CO_20.CO_WSR",
    
    "Active Returns (PERIOD_END >= 202301)": f"""
        SELECT COUNT(*) 
        FROM SCH_CO_20.CO_WSR 
        WHERE PERIOD_END >= {ACTIVE_PERIOD}
    """,
    
    "Active Returns (excluding LSL Credits)": f"""
        SELECT COUNT(*) 
        FROM SCH_CO_20.CO_WSR 
        WHERE PERIOD_END >= {ACTIVE_PERIOD}
        AND EMPLOYER_ID != 23000
    """,
    
    "Unique WSR_IDs (active, excl LSL)": f"""
        SELECT COUNT(DISTINCT WSR_ID) 
        FROM SCH_CO_20.CO_WSR 
        WHERE PERIOD_END >= {ACTIVE_PERIOD}
        AND EMPLOYER_ID != 23000
    """,
}

for label, query in queries.items():
    try:
        cursor.execute(query)
        count = cursor.fetchone()[0]
        print(f"  {label:<45} {count:>12,}")
    except Exception as e:
        print(f"  {label:<45} ERROR: {e}")

print("\n" + "-" * 60)
print("\nPeriod Distribution (last 12 periods):")
print("-" * 60)

cursor.execute(f"""
    SELECT 
        PERIOD_END,
        COUNT(*) as return_count,
        COUNT(DISTINCT EMPLOYER_ID) as unique_employers
    FROM SCH_CO_20.CO_WSR
    WHERE PERIOD_END >= {ACTIVE_PERIOD}
    AND EMPLOYER_ID != 23000
    GROUP BY PERIOD_END
    ORDER BY PERIOD_END DESC
    FETCH FIRST 12 ROWS ONLY
""")

print(f"  {'Period':<12} {'Returns':<15} {'Employers':<15}")
print(f"  {'-'*12} {'-'*15} {'-'*15}")

for row in cursor.fetchall():
    period = row[0]
    returns = row[1]
    employers = row[2]
    print(f"  {period:<12} {returns:>14,} {employers:>14,}")

cursor.close()
conn.close()

print("\n" + "="*60)
print("RECOMMENDATION:")
print("="*60)
print("""
Current setting: LIMIT_ROWS = 10000 (loads first 10K)

Options:
  1. Keep limit for testing: LIMIT_ROWS = 10000
  2. Load all active returns: LIMIT_ROWS = None
  3. Custom limit: LIMIT_ROWS = 50000

To change: Edit sit/sit_return_load.py line 23
""")
