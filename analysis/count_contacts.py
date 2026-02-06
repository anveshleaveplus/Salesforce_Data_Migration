"""
Count contacts that would be loaded for active employers
Quick check before running full load
"""

import os
from dotenv import load_dotenv
import oracledb

load_dotenv('.env.sit')

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

print("\nCounting contacts for active employers...")
print("This may take a few minutes...")

# Count query - simplified version
count_query = """
SELECT COUNT(DISTINCT w.CUSTOMER_ID) as contact_count
FROM SCH_CO_20.CO_WORKER w
INNER JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep 
    ON w.CUSTOMER_ID = ep.WORKER_ID 
    AND ep.EFFECTIVE_TO_DATE IS NULL
WHERE ep.EMPLOYER_ID IN (
    SELECT DISTINCT e.CUSTOMER_ID
    FROM SCH_CO_20.CO_EMPLOYER e
    WHERE EXISTS (
        SELECT 1
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep2
        INNER JOIN SCH_CO_20.CO_SERVICE s 
            ON s.WORKER = ep2.WORKER_ID
        WHERE ep2.EMPLOYER_ID = e.CUSTOMER_ID
            AND s.PERIOD_END >= 202301
    )
)
"""

cursor = conn.cursor()
cursor.execute(count_query)
result = cursor.fetchone()
contact_count = result[0]

print(f"\n✅ Total contacts for active employers: {contact_count:,}")
print(f"   Average per employer: {contact_count/53857:.1f}")
print(f"\n⏱️  Estimated extraction time: {contact_count/10000:.0f}-{contact_count/5000:.0f} minutes")

cursor.close()
conn.close()
