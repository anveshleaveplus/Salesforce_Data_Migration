"""
Test Oracle address concatenation - see how addresses will look in Salesforce
"""
import os
import oracledb
from dotenv import load_dotenv

load_dotenv('.env.sit')

# Connect to Oracle
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
    addr.STREET,
    addr.STREET2,
    addr.SUBURB,
    addr.STATE,
    addr.POSTCODE,
    addr.COUNTRY_CODE,
    TRIM(
        COALESCE(addr.STREET, '') || 
        CASE WHEN addr.STREET2 IS NOT NULL THEN ' ' || addr.STREET2 ELSE '' END || 
        CASE WHEN addr.SUBURB IS NOT NULL THEN ', ' || addr.SUBURB ELSE '' END || 
        CASE WHEN addr.STATE IS NOT NULL THEN ', ' || addr.STATE ELSE '' END || 
        CASE WHEN addr.POSTCODE IS NOT NULL THEN ', ' || addr.POSTCODE ELSE '' END || 
        CASE WHEN addr.COUNTRY_CODE IS NOT NULL THEN ', ' || addr.COUNTRY_CODE ELSE '' END
    ) as FULL_ADDRESS
FROM SCH_CO_20.CO_EMPLOYER e
LEFT JOIN SCH_CO_20.CO_CUSTOMER c
    ON c.CUSTOMER_ID = e.CUSTOMER_ID
LEFT JOIN SCH_CO_20.CO_ADDRESS addr
    ON addr.ADDRESS_ID = c.ADDRESS_ID
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
ORDER BY e.CUSTOMER_ID
"""

cursor = connection.cursor()
cursor.execute(query)

print("\n" + "="*100)
print("ORACLE ADDRESS SAMPLES - How they will appear in Salesforce")
print("="*100)

results = cursor.fetchall()

for i, row in enumerate(results, 1):
    print(f"\n{i}. CUSTOMER_ID: {row[0]}")
    print(f"   Trading Name: {row[1]}")
    print(f"   ---")
    print(f"   Street:       {row[2]}")
    print(f"   Street2:      {row[3]}")
    print(f"   Suburb:       {row[4]}")
    print(f"   State:        {row[5]}")
    print(f"   Postcode:     {row[6]}")
    print(f"   Country:      {row[7]}")
    print(f"   ---")
    print(f"   FULL ADDRESS: {row[8]}")

# Count how many have addresses
cursor.execute("""
SELECT 
    COUNT(*) as Total,
    COUNT(addr.ADDRESS_ID) as Has_Address,
    COUNT(addr.STREET) as Has_Street,
    COUNT(addr.SUBURB) as Has_Suburb,
    COUNT(addr.POSTCODE) as Has_Postcode
FROM SCH_CO_20.CO_EMPLOYER e
LEFT JOIN SCH_CO_20.CO_CUSTOMER c ON c.CUSTOMER_ID = e.CUSTOMER_ID
LEFT JOIN SCH_CO_20.CO_ADDRESS addr ON addr.ADDRESS_ID = c.ADDRESS_ID
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
print("ADDRESS POPULATION STATISTICS")
print("="*100)
print(f"Total Employers:      {stats[0]:,}")
print(f"With Address ID:      {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")
print(f"With Street:          {stats[2]:,} ({stats[2]/stats[0]*100:.1f}%)")
print(f"With Suburb:          {stats[3]:,} ({stats[3]/stats[0]*100:.1f}%)")
print(f"With Postcode:        {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)")
print("="*100)

cursor.close()
connection.close()
