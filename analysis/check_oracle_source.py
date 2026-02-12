"""
Check Oracle data to see what SHOULD have been loaded
"""

import os
from dotenv import load_dotenv
import oracledb

load_dotenv()

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print("=" * 80)
print("CHECKING ORACLE SOURCE DATA")
print("=" * 80)

# Check UnionDelegate field
print("\n[1] UnionDelegate Code Coverage:")
query = """
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN UNION_DELEGATE_CODE IS NOT NULL AND UNION_DELEGATE_CODE != '0' THEN 1 END) as union_delegates
    FROM SCH_CO_20.CO_WORKER
    WHERE CUSTOMER_ID BETWEEN 1000005 AND 1109922
"""
cursor.execute(query)
result = cursor.fetchone()
print(f"  Total workers: {result[0]:,}")
print(f"  Union delegates (not NULL/0): {result[1]:,}")

# Check Phone fields
print("\n[2] Phone Field Coverage:")
query = """
    SELECT 
        COUNT(*) as total,
        COUNT(c.TELEPHONE1_NO) as phone,
        COUNT(c.TELEPHONE2_NO) as other_phone,
        COUNT(c.MOBILE_PHONE_NO) as mobile
    FROM SCH_CO_20.CO_WORKER w
    JOIN SCH_CO_20.CO_CUSTOMER c ON w.CUSTOMER_ID = c.CUSTOMER_ID
    WHERE w.CUSTOMER_ID BETWEEN 1000005 AND 1109922
"""
cursor.execute(query)
result = cursor.fetchone()
print(f"  Total workers: {result[0]:,}")
print(f"  TELEPHONE1_NO: {result[1]:,}")
print(f"  TELEPHONE2_NO: {result[2]:,}")
print(f"  MOBILE_PHONE_NO: {result[3]:,}")

# Sample records
print("\n[3] Sample Records:")
query = """
    SELECT 
        w.CUSTOMER_ID,
        w.UNION_DELEGATE_CODE,
        c.TELEPHONE1_NO,
        c.TELEPHONE2_NO,
        c.MOBILE_PHONE_NO
    FROM SCH_CO_20.CO_WORKER w
    JOIN SCH_CO_20.CO_CUSTOMER c ON w.CUSTOMER_ID = c.CUSTOMER_ID
    WHERE w.CUSTOMER_ID BETWEEN 1000005 AND 1000015
"""
cursor.execute(query)
results = cursor.fetchall()

for row in results:
    print(f"  Worker {row[0]}: Union={row[1]}, Tel1={row[2]}, Tel2={row[3]}, Mobile={row[4]}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("If Oracle has values but SF doesn't, the mapping failed")
