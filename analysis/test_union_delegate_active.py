"""
Test UnionDelegate__c mapping for active contacts (the 50,008 that will be loaded)
"""
import oracledb
import pandas as pd
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

print("="*80)
print("UNION DELEGATE - ACTIVE CONTACTS (50K batch)")
print("="*80)

# Query the same 50K active contacts that will be loaded
ACTIVE_PERIOD = 20231001

query = f"""
SELECT 
    w.CUSTOMER_ID,
    w.UNION_DELEGATE_CODE,
    CASE 
        WHEN w.UNION_DELEGATE_CODE IS NULL OR w.UNION_DELEGATE_CODE = '0' THEN 0
        ELSE 1
    END as CHECKBOX_VALUE
FROM (
    SELECT 
        w.CUSTOMER_ID,
        w.UNION_DELEGATE_CODE,
        ROW_NUMBER() OVER (
            PARTITION BY w.CUSTOMER_ID 
            ORDER BY ep.EFFECTIVE_FROM_DATE DESC NULLS LAST
        ) as rn
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
    INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
    LEFT JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep 
        ON w.CUSTOMER_ID = ep.WORKER_ID 
        AND ep.EFFECTIVE_TO_DATE IS NULL
        AND ep.EMPLOYER_ID IN (
            SELECT DISTINCT e.CUSTOMER_ID
            FROM SCH_CO_20.CO_EMPLOYER e
            WHERE EXISTS (
                SELECT 1
                FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep2
                INNER JOIN SCH_CO_20.CO_SERVICE s 
                    ON s.WORKER = ep2.WORKER_ID
                WHERE ep2.EMPLOYER_ID = e.CUSTOMER_ID
                    AND s.PERIOD_END >= {ACTIVE_PERIOD}
            )
        )
    WHERE w.WORKER_STATUS_ID = 10
) w
WHERE w.rn = 1
"""

cursor = conn.cursor()
cursor.execute(query)
results = cursor.fetchall()

total_active = len(results)
union_delegates = sum(1 for r in results if r[2] == 1)
non_delegates = total_active - union_delegates

print(f"\nTotal Active Contacts (to be loaded): {total_active:,}")
print(f"Union Delegates (True):                {union_delegates:,} ({union_delegates/total_active*100:.2f}%)")
print(f"Non-Delegates (False):                 {non_delegates:,} ({non_delegates/total_active*100:.2f}%)")

# Show distribution by union code for active contacts
union_query = f"""
SELECT 
    w.UNION_DELEGATE_CODE,
    COUNT(*) as count
FROM (
    SELECT 
        w.CUSTOMER_ID,
        w.UNION_DELEGATE_CODE,
        ROW_NUMBER() OVER (
            PARTITION BY w.CUSTOMER_ID 
            ORDER BY ep.EFFECTIVE_FROM_DATE DESC NULLS LAST
        ) as rn
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
    INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
    LEFT JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep 
        ON w.CUSTOMER_ID = ep.WORKER_ID 
        AND ep.EFFECTIVE_TO_DATE IS NULL
        AND ep.EMPLOYER_ID IN (
            SELECT DISTINCT e.CUSTOMER_ID
            FROM SCH_CO_20.CO_EMPLOYER e
            WHERE EXISTS (
                SELECT 1
                FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep2
                INNER JOIN SCH_CO_20.CO_SERVICE s 
                    ON s.WORKER = ep2.WORKER_ID
                WHERE ep2.EMPLOYER_ID = e.CUSTOMER_ID
                    AND s.PERIOD_END >= {ACTIVE_PERIOD}
            )
        )
    WHERE w.WORKER_STATUS_ID = 10
) w
WHERE w.rn = 1
GROUP BY w.UNION_DELEGATE_CODE
ORDER BY COUNT(*) DESC
"""

cursor.execute(union_query)
union_dist = cursor.fetchall()

print("\n" + "="*80)
print(f"{'UNION_CODE':<20} {'COUNT':<15} {'PERCENTAGE':<12} {'\u2192 CHECKBOX':<15}")
print("="*80)

for code, count in union_dist:
    pct = (count/total_active)*100
    display_code = code if code else '0'
    checkbox = 'False' if not code or code == '0' else 'True'
    print(f"{display_code:<20} {count:<15,} {pct:>6.2f}%     {checkbox:<15}")

print("="*80)

# Sample union delegates
print("\nSample Union Delegates (will be True in Salesforce):")
print("-"*60)
delegate_query = f"""
SELECT 
    w.CUSTOMER_ID,
    w.UNION_DELEGATE_CODE
FROM (
    SELECT 
        w.CUSTOMER_ID,
        w.UNION_DELEGATE_CODE,
        ROW_NUMBER() OVER (
            PARTITION BY w.CUSTOMER_ID 
            ORDER BY ep.EFFECTIVE_FROM_DATE DESC NULLS LAST
        ) as rn
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
    INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
    LEFT JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep 
        ON w.CUSTOMER_ID = ep.WORKER_ID 
        AND ep.EFFECTIVE_TO_DATE IS NULL
    WHERE w.WORKER_STATUS_ID = 10
) w
WHERE w.rn = 1 
    AND w.UNION_DELEGATE_CODE IS NOT NULL 
    AND w.UNION_DELEGATE_CODE != '0'
    AND ROWNUM <= 10
"""

cursor.execute(delegate_query)
delegates = cursor.fetchall()

print(f"{'CUSTOMER_ID':<15} {'UNION_CODE':<15} {'\u2192 UnionDelegate__c':<20}")
print("-"*60)
for cid, code in delegates:
    print(f"{cid:<15} {code:<15} {'True':<20}")

print("\n" + "="*80)
print("\u2713 Ready to load UnionDelegate__c field")
print("="*80)

cursor.close()
conn.close()
