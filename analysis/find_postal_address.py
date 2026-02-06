"""
Find Oracle tables with postal address ID columns
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

# Search for columns with 'POSTAL' or 'ADDRESS' in name
query = """
SELECT 
    table_name,
    column_name,
    data_type,
    data_length,
    nullable
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
  AND (
    UPPER(column_name) LIKE '%POSTAL%'
    OR UPPER(column_name) LIKE '%ADDRESS_ID%'
    OR UPPER(column_name) LIKE '%ADDR%'
  )
  AND table_name IN (
    'CO_EMPLOYER', 'CO_CUSTOMER', 'CO_ADDRESS', 'CO_PERSON',
    'CO_WORKER', 'CO_EMPLOYER_CONTACT', 'CO_EMPLOYMENT_PERIOD'
  )
ORDER BY table_name, column_name
"""

cursor = connection.cursor()
cursor.execute(query)

print("\n" + "="*80)
print("ORACLE TABLES WITH POSTAL/ADDRESS COLUMNS")
print("="*80)

results = cursor.fetchall()
current_table = None

for row in results:
    table_name, column_name, data_type, data_length, nullable = row
    
    if table_name != current_table:
        print(f"\n{table_name}")
        print("-"*80)
        current_table = table_name
    
    null_str = "NULL" if nullable == 'Y' else "NOT NULL"
    print(f"  {column_name:30s} {data_type}({data_length}) {null_str}")

# Check specific tables for address-related data
print("\n" + "="*80)
print("SAMPLE DATA FROM KEY TABLES")
print("="*80)

# Check CO_CUSTOMER
print("\nCO_CUSTOMER (first 5 rows):")
print("-"*80)
cursor.execute("""
SELECT CUSTOMER_ID, ADDRESS_ID, POSTAL_ADDRESS_ID
FROM SCH_CO_20.CO_CUSTOMER
WHERE ROWNUM <= 5
""")
for row in cursor.fetchall():
    print(f"  CUSTOMER_ID: {row[0]}, ADDRESS_ID: {row[1]}, POSTAL_ADDRESS_ID: {row[2]}")

# Check if CO_ADDRESS has different types
print("\nCO_ADDRESS structure:")
print("-"*80)
cursor.execute("""
SELECT column_name, data_type
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
  AND table_name = 'CO_ADDRESS'
ORDER BY column_id
""")
for row in cursor.fetchall():
    print(f"  {row[0]:30s} {row[1]}")

# Count usage of POSTAL_ADDRESS_ID vs ADDRESS_ID
print("\n" + "="*80)
print("ADDRESS USAGE STATISTICS")
print("="*80)

cursor.execute("""
SELECT 
    COUNT(*) as Total_Customers,
    COUNT(ADDRESS_ID) as Has_Address_ID,
    COUNT(POSTAL_ADDRESS_ID) as Has_Postal_ID,
    SUM(CASE WHEN ADDRESS_ID = POSTAL_ADDRESS_ID THEN 1 ELSE 0 END) as Same_Address,
    SUM(CASE WHEN ADDRESS_ID != POSTAL_ADDRESS_ID AND POSTAL_ADDRESS_ID IS NOT NULL THEN 1 ELSE 0 END) as Different_Address
FROM SCH_CO_20.CO_CUSTOMER
WHERE CUSTOMER_ID IN (
    SELECT DISTINCT CUSTOMER_ID 
    FROM SCH_CO_20.CO_EMPLOYER
    WHERE CUSTOMER_ID IN (
        SELECT DISTINCT ep.EMPLOYER_ID
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= 202301
        )
    )
)
""")

stats = cursor.fetchone()
print(f"Total Active Customers:           {stats[0]:,}")
print(f"With ADDRESS_ID (registered):     {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")
print(f"With POSTAL_ADDRESS_ID:           {stats[2]:,} ({stats[2]/stats[0]*100:.1f}%)")
print(f"Same as registered address:       {stats[3]:,} ({stats[3]/stats[0]*100:.1f}%)")
print(f"Different postal address:         {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)")

# Sample records with different postal addresses
print("\n" + "="*80)
print("SAMPLE RECORDS WITH DIFFERENT POSTAL ADDRESSES")
print("="*80)

cursor.execute("""
SELECT 
    c.CUSTOMER_ID,
    e.TRADING_NAME,
    a1.STREET || ', ' || a1.SUBURB || ', ' || a1.STATE || ' ' || a1.POSTCODE as Registered_Address,
    a2.STREET || ', ' || a2.SUBURB || ', ' || a2.STATE || ' ' || a2.POSTCODE as Postal_Address
FROM SCH_CO_20.CO_CUSTOMER c
JOIN SCH_CO_20.CO_EMPLOYER e ON e.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN SCH_CO_20.CO_ADDRESS a1 ON a1.ADDRESS_ID = c.ADDRESS_ID
LEFT JOIN SCH_CO_20.CO_ADDRESS a2 ON a2.ADDRESS_ID = c.POSTAL_ADDRESS_ID
WHERE c.ADDRESS_ID != c.POSTAL_ADDRESS_ID
  AND c.POSTAL_ADDRESS_ID IS NOT NULL
  AND c.CUSTOMER_ID IN (
    SELECT DISTINCT ep.EMPLOYER_ID
    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
    WHERE EXISTS (
        SELECT 1 FROM SCH_CO_20.CO_SERVICE s
        WHERE s.WORKER = ep.WORKER_ID
        AND s.PERIOD_END >= 202301
    )
  )
  AND ROWNUM <= 10
""")

for i, row in enumerate(cursor.fetchall(), 1):
    print(f"\n{i}. CUSTOMER_ID: {row[0]}")
    print(f"   Trading Name:        {row[1]}")
    print(f"   Registered Address:  {row[2]}")
    print(f"   Postal Address:      {row[3]}")

cursor.close()
connection.close()

print("\n" + "="*80)
