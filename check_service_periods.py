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

# Check CO_SERVICE structure
print("CO_SERVICE table structure:")
print("="*80)
cursor.execute("""
    SELECT column_name, data_type 
    FROM all_tab_columns 
    WHERE owner = 'SCH_CO_20' 
    AND table_name = 'CO_SERVICE' 
    ORDER BY column_id
""")
for row in cursor.fetchall():
    print(f'  {row[0]:<40} {row[1]}')

# Sample data - PERIOD_END/START are NUMBER (YYYYMM format)
print("\n\nSample CO_SERVICE records:")
print("="*80)
cursor.execute("""
    SELECT SERVICE_ID, WORKER, PERIOD_START, PERIOD_END
    FROM SCH_CO_20.CO_SERVICE 
    WHERE ROWNUM <= 10
    ORDER BY PERIOD_END DESC NULLS LAST
""")
for row in cursor.fetchall():
    print(f'  Service {row[0]}: Worker {row[1]}, Period: {row[2]} to {row[3]}')

# Count by period year - periods are YYYYMM format
print("\n\nEmployers by last service period year (from CO_SERVICE via CO_EMPLOYMENT_PERIOD):")
print("="*80)
cursor.execute("""
    SELECT FLOOR(s.PERIOD_END/100) as year,
           COUNT(DISTINCT ep.EMPLOYER_ID) as employer_count
    FROM SCH_CO_20.CO_SERVICE s
    JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep ON s.WORKER = ep.WORKER_ID
    WHERE s.PERIOD_END IS NOT NULL
    GROUP BY FLOOR(s.PERIOD_END/100)
    ORDER BY year DESC
""")
for row in cursor.fetchall()[:20]:
    year = int(row[0]) if row[0] else 'NULL'
    count = row[1]
    marker = ' ← 2023+' if year and year >= 2023 else ''
    print(f'  {year}: {count:,} employers{marker}')

# Count active employers with service in 2023+
print("\n\nActive Employers (with service records):")
print("="*80)
cursor.execute("""
    SELECT COUNT(DISTINCT ep.EMPLOYER_ID) 
    FROM SCH_CO_20.CO_SERVICE s
    JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep ON s.WORKER = ep.WORKER_ID
    WHERE s.PERIOD_END >= 202301
""")
count_2023 = cursor.fetchone()[0]
print(f"PERIOD_END >= 202301 (Jan 2023): {count_2023:,} employers")

cursor.execute("""
    SELECT COUNT(DISTINCT ep.EMPLOYER_ID) 
    FROM SCH_CO_20.CO_SERVICE s
    JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep ON s.WORKER = ep.WORKER_ID
    WHERE s.PERIOD_END >= 202401
""")
count_2024 = cursor.fetchone()[0]
print(f"PERIOD_END >= 202401 (Jan 2024): {count_2024:,} employers")

cursor.execute("""
    SELECT COUNT(DISTINCT ep.EMPLOYER_ID) 
    FROM SCH_CO_20.CO_SERVICE s
    JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep ON s.WORKER = ep.WORKER_ID
    WHERE s.PERIOD_END >= 202201
""")
count_2022 = cursor.fetchone()[0]
print(f"PERIOD_END >= 202201 (Jan 2022): {count_2022:,} employers")

conn.close()
