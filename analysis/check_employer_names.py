"""
Query sample values for CO_EMPLOYER name fields
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

print("\nCO_EMPLOYER Name Fields - Sample Values")
print("="*80)

# First, check what columns exist
column_query = """
SELECT column_name 
FROM all_tab_columns 
WHERE table_name = 'CO_EMPLOYER' 
AND owner = 'SCH_CO_20'
AND column_name LIKE '%NAME%'
ORDER BY column_name
"""

cursor = conn.cursor()
cursor.execute(column_query)
print("\nAvailable NAME columns in CO_EMPLOYER:")
name_columns = []
for row in cursor:
    print(f"  - {row[0]}")
    name_columns.append(row[0])
cursor.close()

print("\n" + "="*80)

# Now query with actual columns
query = f"""
SELECT 
    CUSTOMER_ID,
    TRADING_NAME,
    ENTITY_NAME,
    COMPANY_NAME,
    OTHER_BUSINESS_NAME
FROM SCH_CO_20.CO_EMPLOYER
WHERE ROWNUM <= 20
ORDER BY CUSTOMER_ID
"""

cursor = conn.cursor()
cursor.execute(query)

print(f"\n{'ID':<8} {'TRADING_NAME':<25} {'ENTITY_NAME':<25} {'COMPANY_NAME':<25} {'OTHER_BIZ_NAME':<25}")
print("-"*115)

for row in cursor:
    customer_id = row[0]
    trading_name = row[1] or 'NULL'
    entity_name = row[2] or 'NULL'
    company_name = row[3] or 'NULL'
    other_biz = row[4] or 'NULL'
    
    print(f"{customer_id:<8} {str(trading_name)[:24]:<25} {str(entity_name)[:24]:<25} {str(company_name)[:24]:<25} {str(other_biz)[:24]:<25}")

cursor.close()

# Get counts of non-null values
count_query = """
SELECT 
    COUNT(*) as total,
    COUNT(TRADING_NAME) as trading_name_count,
    COUNT(ENTITY_NAME) as entity_name_count,
    COUNT(COMPANY_NAME) as company_name_count,
    COUNT(OTHER_BUSINESS_NAME) as other_biz_count
FROM SCH_CO_20.CO_EMPLOYER
"""

cursor = conn.cursor()
cursor.execute(count_query)
result = cursor.fetchone()

print("\n" + "="*80)
print("Population Statistics:")
print(f"  Total Records: {result[0]:,}")
print(f"  TRADING_NAME populated: {result[1]:,} ({result[1]/result[0]*100:.1f}%)")
print(f"  ENTITY_NAME populated: {result[2]:,} ({result[2]/result[0]*100:.1f}%)")
print(f"  COMPANY_NAME populated: {result[3]:,} ({result[3]/result[0]*100:.1f}%)")
print(f"  OTHER_BUSINESS_NAME populated: {result[4]:,} ({result[4]/result[0]*100:.1f}%)")

cursor.close()
conn.close()
