"""
Test IS_POSTAL_DIFFERENT logic
"""
import os
import oracledb
from dotenv import load_dotenv

load_dotenv('.env.sit')

connection = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

query = """
SELECT 
    e.CUSTOMER_ID,
    e.TRADING_NAME,
    c.ADDRESS_ID,
    c.POSTAL_ADDRESS_ID,
    CASE 
        WHEN c.POSTAL_ADDRESS_ID IS NOT NULL 
            AND c.ADDRESS_ID != c.POSTAL_ADDRESS_ID 
        THEN 1 
        ELSE 0 
    END as IS_POSTAL_DIFFERENT
FROM SCH_CO_20.CO_EMPLOYER e
LEFT JOIN SCH_CO_20.CO_CUSTOMER c
    ON c.CUSTOMER_ID = e.CUSTOMER_ID
WHERE e.CUSTOMER_ID IN (
    SELECT DISTINCT ep.EMPLOYER_ID
    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
    WHERE EXISTS (
        SELECT 1 FROM SCH_CO_20.CO_SERVICE s
        WHERE s.WORKER = ep.WORKER_ID
        AND s.PERIOD_END >= 202301
    )
)
AND ROWNUM <= 20
ORDER BY IS_POSTAL_DIFFERENT DESC, e.CUSTOMER_ID
"""

cursor = connection.cursor()
cursor.execute(query)

print("\n" + "="*100)
print("IS_POSTAL_DIFFERENT Field Test")
print("="*100)

results = cursor.fetchall()

print("\nRecords with DIFFERENT postal addresses (IS_POSTAL_DIFFERENT = 1):")
print("-"*100)
for row in results:
    if row[4] == 1:
        print(f"CUSTOMER_ID: {row[0]:10} | {row[1]:50s} | Addr ID: {row[2]:6} vs Postal: {row[3]:6} | Flag: {row[4]}")

print("\nRecords with SAME/NO postal address (IS_POSTAL_DIFFERENT = 0):")
print("-"*100)
for row in results:
    if row[4] == 0:
        postal_str = str(row[3]) if row[3] else "NULL"
        print(f"CUSTOMER_ID: {row[0]:10} | {row[1]:50s} | Addr ID: {row[2]:6} vs Postal: {postal_str:6} | Flag: {row[4]}")

# Count statistics
cursor.execute("""
SELECT 
    COUNT(*) as Total,
    SUM(CASE WHEN c.POSTAL_ADDRESS_ID IS NOT NULL AND c.ADDRESS_ID != c.POSTAL_ADDRESS_ID THEN 1 ELSE 0 END) as Different,
    SUM(CASE WHEN c.POSTAL_ADDRESS_ID IS NULL THEN 1 ELSE 0 END) as No_Postal,
    SUM(CASE WHEN c.POSTAL_ADDRESS_ID = c.ADDRESS_ID THEN 1 ELSE 0 END) as Same
FROM SCH_CO_20.CO_EMPLOYER e
LEFT JOIN SCH_CO_20.CO_CUSTOMER c ON c.CUSTOMER_ID = e.CUSTOMER_ID
WHERE e.CUSTOMER_ID IN (
    SELECT DISTINCT ep.EMPLOYER_ID
    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
    WHERE EXISTS (
        SELECT 1 FROM SCH_CO_20.CO_SERVICE s
        WHERE s.WORKER = ep.WORKER_ID
        AND s.PERIOD_END >= 202301
    )
)
""")

stats = cursor.fetchone()

print("\n" + "="*100)
print("STATISTICS")
print("="*100)
print(f"Total Active Employers:           {stats[0]:,}")
print(f"Different Postal Address (=1):    {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")
print(f"No Postal Address (=0):           {stats[2]:,} ({stats[2]/stats[0]*100:.1f}%)")
print(f"Same as Registered (=0):          {stats[3]:,} ({stats[3]/stats[0]*100:.1f}%)")
print(f"\nExpected TRUE in SF:              {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")
print(f"Expected FALSE in SF:             {stats[0]-stats[1]:,} ({(stats[0]-stats[1])/stats[0]*100:.1f}%)")

cursor.close()
connection.close()
