"""
Check phone/union data across ALL customer ID ranges
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
print("CHECKING ALL ACTIVE CONTACTS DATA COVERAGE")
print("=" * 80)

# Check UnionDelegate across ALL active contacts
print("\n[1] UnionDelegate Code Coverage (ALL 176K active):")
query = """
    SELECT 
        COUNT(DISTINCT w.CUSTOMER_ID) as total,
        COUNT(DISTINCT CASE WHEN w.UNION_DELEGATE_CODE IS NOT NULL AND w.UNION_DELEGATE_CODE != '0' THEN w.CUSTOMER_ID END) as union_delegates
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_SERVICE s ON s.WORKER = w.CUSTOMER_ID
    WHERE s.PERIOD_END >= 202301
"""
cursor.execute(query)
result = cursor.fetchone()
print(f"  Total active workers: {result[0]:,}")
print(f"  Union delegates: {result[1]:,} ({result[1]/result[0]*100:.2f}%)")

# Check Phone fields across ALL active contacts
print("\n[2] Phone Field Coverage (ALL active):")
query = """
    SELECT 
        COUNT(DISTINCT w.CUSTOMER_ID) as total,
        COUNT(DISTINCT CASE WHEN c.TELEPHONE1_NO IS NOT NULL THEN w.CUSTOMER_ID END) as phone,
        COUNT(DISTINCT CASE WHEN c.TELEPHONE2_NO IS NOT NULL THEN w.CUSTOMER_ID END) as other_phone,
        COUNT(DISTINCT CASE WHEN c.MOBILE_PHONE_NO IS NOT NULL THEN w.CUSTOMER_ID END) as mobile
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_SERVICE s ON s.WORKER = w.CUSTOMER_ID
    JOIN SCH_CO_20.CO_CUSTOMER c ON w.CUSTOMER_ID = c.CUSTOMER_ID
    WHERE s.PERIOD_END >= 202301
"""
cursor.execute(query)
result = cursor.fetchone()
total = result[0]
print(f"  Total: {total:,}")
print(f"  TELEPHONE1_NO: {result[1]:,} ({result[1]/total*100:.2f}%)")
print(f"  TELEPHONE2_NO: {result[2]:,} ({result[2]/total*100:.2f}%)")
print(f"  MOBILE_PHONE_NO: {result[3]:,} ({result[3]/total*100:.2f}%)")

# Find CUSTOMER_ID ranges with data
print("\n[3] Finding ID ranges WITH phone data:")
query = """
    SELECT 
        FLOOR(w.CUSTOMER_ID / 10000) * 10000 as range_start,
        COUNT(DISTINCT w.CUSTOMER_ID) as total,
        COUNT(DISTINCT CASE WHEN c.MOBILE_PHONE_NO IS NOT NULL THEN w.CUSTOMER_ID END) as has_mobile,
        COUNT(DISTINCT CASE WHEN c.TELEPHONE1_NO IS NOT NULL THEN w.CUSTOMER_ID END) as has_phone
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_SERVICE s ON s.WORKER = w.CUSTOMER_ID
    JOIN SCH_CO_20.CO_CUSTOMER c ON w.CUSTOMER_ID = c.CUSTOMER_ID
    WHERE s.PERIOD_END >= 202301
    GROUP BY FLOOR(w.CUSTOMER_ID / 10000)
    HAVING COUNT(DISTINCT CASE WHEN c.MOBILE_PHONE_NO IS NOT NULL THEN w.CUSTOMER_ID END) > 0 
        OR COUNT(DISTINCT CASE WHEN c.TELEPHONE1_NO IS NOT NULL THEN w.CUSTOMER_ID END) > 0
    ORDER BY range_start
"""
cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"\n  Ranges with phone data:")
    for row in results:
        range_start = row[0]
        range_end = range_start + 9999
        print(f"    {range_start:,}-{range_end:,}: {row[1]:,} workers, {row[2]:,} mobile, {row[3]:,} landline")
else:
    print("  ❌ NO ranges have phone data!")

# Sample from highest coverage range
if results:
    best_range = results[0][0]
    print(f"\n[4] Sample from range {best_range:,}:")
    query = f"""
        SELECT 
            w.CUSTOMER_ID,
            w.UNION_DELEGATE_CODE,
            c.TELEPHONE1_NO,
            c.MOBILE_PHONE_NO
        FROM SCH_CO_20.CO_WORKER w
        INNER JOIN SCH_CO_20.CO_SERVICE s ON s.WORKER = w.CUSTOMER_ID
        JOIN SCH_CO_20.CO_CUSTOMER c ON w.CUSTOMER_ID = c.CUSTOMER_ID
        WHERE w.CUSTOMER_ID BETWEEN {best_range} AND {best_range + 9999}
        AND s.PERIOD_END >= 202301
        AND (c.MOBILE_PHONE_NO IS NOT NULL OR c.TELEPHONE1_NO IS NOT NULL)
        AND ROWNUM <= 10
    """
    cursor.execute(query)
    samples = cursor.fetchall()
    for row in samples:
        print(f"    Worker {row[0]}: Union={row[1]}, Phone={row[2]}, Mobile={row[3]}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("CONCLUSION:")
if results:
    print("✅ Some ranges have data - loading ALL contacts might get better coverage!")
else:
    print("❌ Oracle SIT has NO phone/union data across ALL contacts")
