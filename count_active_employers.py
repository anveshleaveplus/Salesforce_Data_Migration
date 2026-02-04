import oracledb
from dotenv import load_dotenv
import os

load_dotenv()

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print("Active Employer Counts:")
print("="*80)

# Method 1: Count distinct employer IDs from CO_EMPLOYMENT_PERIOD where workers have recent service
print("\nMethod 1: Via CO_EMPLOYMENT_PERIOD + CO_SERVICE join")
print("-"*80)
cursor.execute("""
    SELECT COUNT(DISTINCT ep.EMPLOYER_ID) 
    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
    WHERE EXISTS (
        SELECT 1 FROM SCH_CO_20.CO_SERVICE s
        WHERE s.WORKER = ep.WORKER_ID
        AND s.PERIOD_END >= 202301
    )
""")
count = cursor.fetchone()[0]
print(f"Employers with workers having service >= 202301: {count:,}")

# Alternative: Check recent period ranges
print("\n\nComparison with different date ranges:")
print("-"*80)

for year_month in [202401, 202301, 202201, 202101]:
    year = year_month // 100
    month = year_month % 100
    cursor.execute(f"""
        SELECT COUNT(DISTINCT ep.EMPLOYER_ID) 
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= {year_month}
        )
    """)
    count = cursor.fetchone()[0]
    print(f"  >= {year_month} ({year}-{month:02d}): {count:,} employers")

# For comparison: Total employers
print("\n\nFor comparison:")
print("-"*80)
cursor.execute("SELECT COUNT(*) FROM SCH_CO_20.CO_EMPLOYER")
total = cursor.fetchone()[0]
print(f"Total employers in CO_EMPLOYER: {total:,}")

cursor.execute("""
    SELECT COUNT(*) FROM SCH_CO_20.CO_EMPLOYER
    WHERE FIRST_EMPLOYED_WORKERS_DATE >= TO_DATE('2015-01-01', 'YYYY-MM-DD')
""")
since_2015 = cursor.fetchone()[0]
print(f"FIRST_EMPLOYED_WORKERS_DATE >= 2015: {since_2015:,}")

conn.close()
