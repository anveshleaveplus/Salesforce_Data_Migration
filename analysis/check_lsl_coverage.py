"""
Check CO_WORKER_ENTITLEMENT_V4.LSL_DUE coverage for active workers
This is the real-time calculation view (not stale data)
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

ACTIVE_PERIOD = 202301
LIMIT_ROWS = 50000

print("=" * 80)
print("CO_WORKER_ENTITLEMENT_V4.LSL_DUE COVERAGE CHECK")
print("=" * 80)
print("Real-time calculation view (not stale data)")

# [1] Coverage for 50K active workers
print("\n[1] Active Worker Coverage (50K load):")
print("-" * 80)

query = f"""
    SELECT 
        COUNT(DISTINCT w.CUSTOMER_ID) as total_workers,
        COUNT(DISTINCT ent.CUSTOMER_ID) as with_entitlement_record,
        COUNT(DISTINCT CASE WHEN ent.LSL_DUE IS NOT NULL THEN ent.CUSTOMER_ID END) as with_lsl_due_not_null,
        COUNT(DISTINCT CASE WHEN ent.LSL_DUE > 0 THEN ent.CUSTOMER_ID END) as with_positive_lsl,
        COUNT(DISTINCT CASE WHEN ent.LSL_DUE < 0 THEN ent.CUSTOMER_ID END) as with_negative_lsl
    FROM (
        SELECT DISTINCT w.CUSTOMER_ID
        FROM CO_WORKER w
        INNER JOIN CO_EMPLOYMENT_PERIOD ep ON w.CUSTOMER_ID = ep.WORKER_ID
        WHERE ep.EFFECTIVE_TO_DATE IS NULL
        AND ep.EMPLOYER_ID IN (
            SELECT DISTINCT e.CUSTOMER_ID
            FROM CO_EMPLOYER e
            WHERE EXISTS (
                SELECT 1
                FROM CO_EMPLOYMENT_PERIOD ep2
                INNER JOIN CO_SERVICE s ON s.WORKER = ep2.WORKER_ID
                WHERE ep2.EMPLOYER_ID = e.CUSTOMER_ID
                AND s.PERIOD_END >= {ACTIVE_PERIOD}
            )
        )
        AND ROWNUM <= {LIMIT_ROWS}
    ) w
    LEFT JOIN CO_WORKER_ENTITLEMENT_V4 ent ON w.CUSTOMER_ID = ent.CUSTOMER_ID
"""

cursor.execute(query)
result = cursor.fetchone()

if result:
    total, with_ent, with_lsl_not_null, with_positive, with_negative = result
    print(f"Total active workers (query): {total:,}")
    print(f"With entitlement record: {with_ent:,} ({with_ent/total*100:.1f}%)")
    print(f"With LSL_DUE not null: {with_lsl_not_null:,} ({with_lsl_not_null/total*100:.1f}%)")
    print(f"With LSL_DUE > 0: {with_positive:,} ({with_positive/total*100:.1f}%)")
    print(f"With LSL_DUE < 0: {with_negative:,} ({with_negative/total*100:.1f}%)")
    print(f"With LSL_DUE = 0: {with_lsl_not_null - with_positive - with_negative:,}")

# [2] Value ranges
print("\n[2] LSL_DUE Value Distribution:")
print("-" * 80)

query = f"""
    SELECT 
        MIN(ent.LSL_DUE) as min_lsl,
        MAX(ent.LSL_DUE) as max_lsl,
        AVG(ent.LSL_DUE) as avg_lsl,
        MEDIAN(ent.LSL_DUE) as median_lsl,
        STDDEV(ent.LSL_DUE) as stddev_lsl
    FROM (
        SELECT DISTINCT w.CUSTOMER_ID
        FROM CO_WORKER w
        INNER JOIN CO_EMPLOYMENT_PERIOD ep ON w.CUSTOMER_ID = ep.WORKER_ID
        WHERE ep.EFFECTIVE_TO_DATE IS NULL
        AND ep.EMPLOYER_ID IN (
            SELECT DISTINCT e.CUSTOMER_ID
            FROM CO_EMPLOYER e
            WHERE EXISTS (
                SELECT 1
                FROM CO_EMPLOYMENT_PERIOD ep2
                INNER JOIN CO_SERVICE s ON s.WORKER = ep2.WORKER_ID
                WHERE ep2.EMPLOYER_ID = e.CUSTOMER_ID
                AND s.PERIOD_END >= {ACTIVE_PERIOD}
            )
        )
        AND ROWNUM <= {LIMIT_ROWS}
    ) w
    INNER JOIN CO_WORKER_ENTITLEMENT_V4 ent ON w.CUSTOMER_ID = ent.CUSTOMER_ID
    WHERE ent.LSL_DUE IS NOT NULL
"""

cursor.execute(query)
result = cursor.fetchone()

if result:
    min_lsl, max_lsl, avg_lsl, median_lsl, stddev_lsl = result
    print(f"Min LSL_DUE: {min_lsl:.2f} days")
    print(f"Max LSL_DUE: {max_lsl:.2f} days")
    print(f"Avg LSL_DUE: {avg_lsl:.2f} days")
    print(f"Median LSL_DUE: {median_lsl:.2f} days")
    print(f"Std Dev: {stddev_lsl:.2f} days")

# [3] Sample data
print("\n[3] Sample Worker LSL_DUE Values:")
print("-" * 80)

query = f"""
    SELECT 
        w.CUSTOMER_ID,
        ent.LSL_DUE,
        ROUND(ent.LSL_DUE) as rounded_lsl,
        p.FIRST_NAME,
        p.SURNAME
    FROM (
        SELECT DISTINCT w.CUSTOMER_ID
        FROM CO_WORKER w
        INNER JOIN CO_EMPLOYMENT_PERIOD ep ON w.CUSTOMER_ID = ep.WORKER_ID
        WHERE ep.EFFECTIVE_TO_DATE IS NULL
        AND ep.EMPLOYER_ID IN (
            SELECT DISTINCT e.CUSTOMER_ID
            FROM CO_EMPLOYER e
            WHERE EXISTS (
                SELECT 1
                FROM CO_EMPLOYMENT_PERIOD ep2
                INNER JOIN CO_SERVICE s ON s.WORKER = ep2.WORKER_ID
                WHERE ep2.EMPLOYER_ID = e.CUSTOMER_ID
                AND s.PERIOD_END >= {ACTIVE_PERIOD}
            )
        )
        AND ROWNUM <= {LIMIT_ROWS}
    ) w
    INNER JOIN CO_WORKER_ENTITLEMENT_V4 ent ON w.CUSTOMER_ID = ent.CUSTOMER_ID
    INNER JOIN CO_PERSON p ON w.CUSTOMER_ID = p.PERSON_ID
    WHERE ROWNUM <= 15
    ORDER BY ent.LSL_DUE DESC
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"{'CUSTOMER_ID':<12} {'LSL_DUE':<12} {'ROUNDED':<10} {'Name':<30}")
    print("-" * 66)
    for cust_id, lsl_due, rounded, first, last in results:
        name = f"{first or ''} {last or ''}".strip()
        print(f"{cust_id:<12} {lsl_due:<12.2f} {int(rounded):<10} {name:<30}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("RECOMMENDATION")
print("=" * 80)
print("\nMapping: Contact.CurrentLeaveBalance__c ← ROUND(CO_WORKER_ENTITLEMENT_V4.LSL_DUE)")
print("\nImportant:")
print("  - LSL_DUE is calculated in real-time in Oracle")
print("  - Salesforce will store a SNAPSHOT that becomes stale")
print("  - Needs periodic reload to stay current")
print("  - Consider showing 'as of date' to users in SF")
