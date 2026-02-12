"""
Check union delegate code values in CO_WORKER
"""
import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

# Oracle connection
conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print("="*80)
print("UNION DELEGATE CODE ANALYSIS")
print("="*80)

# Check values and counts
query = """
SELECT union_delegate_code, COUNT(1) as count
FROM SCH_CO_20.CO_WORKER
GROUP BY union_delegate_code
ORDER BY COUNT(1) DESC
"""

cursor.execute(query)
results = cursor.fetchall()

print(f"\nTotal distinct values: {len(results)}")
print("\nValue Distribution:")
print("-"*80)
print(f"{'UNION_DELEGATE_CODE':<30} {'COUNT':<15} {'PERCENTAGE':<15}")
print("-"*80)

total = sum(r[1] for r in results)
for value, count in results:
    pct = (count/total)*100
    display_val = str(value) if value is not None else 'NULL'
    print(f"{display_val:<30} {count:<15,} {pct:>6.2f}%")

print("-"*80)
print(f"{'TOTAL':<30} {total:<15,} {'100.00%':>6}")
print("="*80)

# Sample records
print("\nSample records with UNION_DELEGATE_CODE:")
sample_query = """
SELECT CUSTOMER_ID, UNION_DELEGATE_CODE
FROM SCH_CO_20.CO_WORKER
WHERE UNION_DELEGATE_CODE IS NOT NULL
AND ROWNUM <= 10
"""
cursor.execute(sample_query)
samples = cursor.fetchall()

print(f"\n{'CUSTOMER_ID':<15} {'UNION_DELEGATE_CODE':<20}")
print("-"*40)
for cid, code in samples:
    print(f"{cid:<15} {code:<20}")

cursor.close()
conn.close()
