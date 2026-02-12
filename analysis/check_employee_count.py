"""
Check if we can count employees per employer from CO_EMPLOYMENT_PERIOD table
This verifies the possibility of mapping NumberOfEmployees field
"""
import oracledb
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv('.env.sit')

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print("Checking Employee Count Mapping Feasibility")
print("="*80)

# Test 1: Count total employment periods
print("\n1. Total employment period records:")
cursor.execute("SELECT COUNT(*) FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD")
total = cursor.fetchone()[0]
print(f"   Total records in CO_EMPLOYMENT_PERIOD: {total:,}")

# Test 2: Sample employer with employee count
print("\n2. Sample employers with employee counts:")
query = """
SELECT 
    ep.EMPLOYER_ID,
    e.TRADING_NAME,
    COUNT(DISTINCT ep.WORKER_ID) as EMPLOYEE_COUNT,
    COUNT(*) as TOTAL_EMPLOYMENT_PERIODS
FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
LEFT JOIN SCH_CO_20.CO_EMPLOYER e ON e.CUSTOMER_ID = ep.EMPLOYER_ID
WHERE ep.EMPLOYER_ID IN (
    SELECT DISTINCT EMPLOYER_ID 
    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD 
    WHERE ROWNUM <= 100
)
GROUP BY ep.EMPLOYER_ID, e.TRADING_NAME
ORDER BY EMPLOYEE_COUNT DESC
FETCH FIRST 10 ROWS ONLY
"""
df = pd.read_sql(query, conn)
print(df.to_string(index=False))

# Test 3: Count only active employees (with recent service)
print("\n3. Active employees only (with service >= 202301):")
query = """
SELECT 
    ep.EMPLOYER_ID,
    e.TRADING_NAME,
    COUNT(DISTINCT ep.WORKER_ID) as ACTIVE_EMPLOYEE_COUNT
FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
LEFT JOIN SCH_CO_20.CO_EMPLOYER e ON e.CUSTOMER_ID = ep.EMPLOYER_ID
WHERE EXISTS (
    SELECT 1 FROM SCH_CO_20.CO_SERVICE s
    WHERE s.WORKER = ep.WORKER_ID
    AND s.PERIOD_END >= 202301
)
GROUP BY ep.EMPLOYER_ID, e.TRADING_NAME
ORDER BY ACTIVE_EMPLOYEE_COUNT DESC
FETCH FIRST 10 ROWS ONLY
"""
df = pd.read_sql(query, conn)
print(df.to_string(index=False))

# Test 4: Check if there are date-based filters we should consider
print("\n4. Checking employment period date ranges:")
cursor.execute("""
SELECT 
    COUNT(*) as TOTAL_RECORDS,
    COUNT(CASE WHEN EFFECTIVE_FROM_DATE IS NOT NULL THEN 1 END) as HAS_START_DATE,
    COUNT(CASE WHEN EFFECTIVE_TO_DATE IS NOT NULL THEN 1 END) as HAS_END_DATE,
    COUNT(CASE WHEN EFFECTIVE_TO_DATE IS NULL THEN 1 END) as STILL_EMPLOYED
FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD
""")
result = cursor.fetchone()
print(f"   Total records: {result[0]:,}")
print(f"   With start date: {result[1]:,}")
print(f"   With end date: {result[2]:,}")
print(f"   Still employed (no end date): {result[3]:,}")

# Test 5: Distribution of employee counts
print("\n5. Distribution of employee counts per employer:")
cursor.execute("""
SELECT 
    EMPLOYEE_COUNT_BUCKET,
    COUNT(*) as EMPLOYER_COUNT
FROM (
    SELECT 
        EMPLOYER_ID,
        CASE 
            WHEN COUNT(DISTINCT WORKER_ID) = 1 THEN '1'
            WHEN COUNT(DISTINCT WORKER_ID) BETWEEN 2 AND 5 THEN '2-5'
            WHEN COUNT(DISTINCT WORKER_ID) BETWEEN 6 AND 10 THEN '6-10'
            WHEN COUNT(DISTINCT WORKER_ID) BETWEEN 11 AND 50 THEN '11-50'
            WHEN COUNT(DISTINCT WORKER_ID) BETWEEN 51 AND 100 THEN '51-100'
            ELSE '100+'
        END as EMPLOYEE_COUNT_BUCKET
    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD
    GROUP BY EMPLOYER_ID
)
GROUP BY EMPLOYEE_COUNT_BUCKET
ORDER BY 
    CASE EMPLOYEE_COUNT_BUCKET
        WHEN '1' THEN 1
        WHEN '2-5' THEN 2
        WHEN '6-10' THEN 3
        WHEN '11-50' THEN 4
        WHEN '51-100' THEN 5
        ELSE 6
    END
""")
print(f"{'Employees':<15} {'Employers':<15}")
print("-"*30)
for row in cursor:
    print(f"{row[0]:<15} {row[1]:,}")

conn.close()

print("\n" + "="*80)
print("CONCLUSION:")
print("✓ YES - We can count employees per employer from CO_EMPLOYMENT_PERIOD")
print("✓ Use: COUNT(DISTINCT WORKER_ID) grouped by EMPLOYER_ID")
print("✓ Consider filtering for active employees only (with recent service)")
print("="*80)
