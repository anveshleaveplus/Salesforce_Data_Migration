"""
Simple LSL coverage check for 50K active workers
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

print("LSL_DUE Coverage for 50K Active Workers")
print("=" * 60)

query = """
    SELECT 
        COUNT(*) as total,
        COUNT(ent.LSL_DUE) as with_lsl,
        ROUND(AVG(ent.LSL_DUE), 2) as avg_lsl,
        ROUND(MIN(ent.LSL_DUE), 2) as min_lsl,
        ROUND(MAX(ent.LSL_DUE), 2) as max_lsl
    FROM (
        SELECT w.CUSTOMER_ID
        FROM CO_WORKER w
        INNER JOIN CO_EMPLOYMENT_PERIOD ep ON w.CUSTOMER_ID = ep.WORKER_ID
        WHERE ep.EFFECTIVE_TO_DATE IS NULL
        AND ROWNUM <= 50000
    ) w
    LEFT JOIN CO_WORKER_ENTITLEMENT_V4 ent ON w.CUSTOMER_ID = ent.CUSTOMER_ID
"""

cursor.execute(query)
result = cursor.fetchone()

if result:
    total, with_lsl, avg, min_val, max_val = result
    coverage_pct = (with_lsl / total * 100) if total > 0 else 0
    
    print(f"\nTotal workers queried: {total:,}")
    print(f"With LSL_DUE value: {with_lsl:,} ({coverage_pct:.1f}%)")
    print(f"\nLSL_DUE ranges:")
    print(f"  Min: {min_val} days")
    print(f"  Avg: {avg} days")
    print(f"  Max: {max_val} days")

cursor.close()
conn.close()

print("\n" + "=" * 60)
print("Mapping: CurrentLeaveBalance__c ← ROUND(LSL_DUE)")
print("Note: Salesforce stores snapshot, needs periodic reload")
