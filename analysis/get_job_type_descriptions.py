"""
Get descriptions for JobClassificationCode and WorkerTypeCode
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
print("JOB CLASSIFICATION AND WORKER TYPE CODE DESCRIPTIONS")
print("=" * 80)

# [1] Code Set 6: JobClassificationCode
print("\n[1] Code Set 6 - JobClassificationCode:")
print("-" * 80)

query = """
    SELECT VALUE, DESCRIPTION
    FROM SCH_CO_20.CO_CODE
    WHERE CODE_SET_ID = 6
    ORDER BY VALUE
"""

try:
    cursor.execute(query)
    results = cursor.fetchall()
    
    if results:
        print(f"Found {len(results)} job classification codes:")
        print(f"{'Code':<8} {'Description':<60}")
        print("-" * 70)
        for code, desc in results:
            print(f"{str(code):<8} {desc or 'N/A':<60}")
    else:
        print("No codes found")
except Exception as e:
    print(f"Error: {e}")

# [2] Code Set 25: WorkerTypeCode
print("\n[2] Code Set 25 - WorkerTypeCode:")
print("-" * 80)

query = """
    SELECT VALUE, DESCRIPTION
    FROM SCH_CO_20.CO_CODE
    WHERE CODE_SET_ID = 25
    ORDER BY VALUE
"""

try:
    cursor.execute(query)
    results = cursor.fetchall()
    
    if results:
        print(f"Found {len(results)} worker type codes:")
        print(f"{'Code':<8} {'Description':<60}")
        print("-" * 70)
        for code, desc in results:
            print(f"{str(code):<8} {desc or 'N/A':<60}")
    else:
        print("No codes found")
except Exception as e:
    print(f"Error: {e}")

# [3] Check active worker coverage
print("\n[3] Active Worker Coverage (50K contacts):")
print("-" * 80)

ACTIVE_PERIOD = 202301

query = f"""
    SELECT 
        COUNT(DISTINCT w.CUSTOMER_ID) as total_workers,
        COUNT(DISTINCT CASE WHEN w.WORKER_TYPE_CODE IS NOT NULL THEN w.CUSTOMER_ID END) as with_worker_type,
        COUNT(DISTINCT CASE WHEN ep.JOB_CLASSIFICATION_CODE IS NOT NULL THEN w.CUSTOMER_ID END) as with_job_class
    FROM CO_WORKER w
    LEFT JOIN CO_EMPLOYMENT_PERIOD ep 
        ON w.CUSTOMER_ID = ep.WORKER_ID 
        AND ep.EFFECTIVE_TO_DATE IS NULL
        AND ep.EMPLOYER_ID IN (
            SELECT DISTINCT e.CUSTOMER_ID
            FROM CO_EMPLOYER e
            WHERE EXISTS (
                SELECT 1
                FROM CO_EMPLOYMENT_PERIOD ep2
                INNER JOIN CO_SERVICE s 
                    ON s.WORKER = ep2.WORKER_ID
                WHERE ep2.EMPLOYER_ID = e.CUSTOMER_ID
                    AND s.PERIOD_END >= {ACTIVE_PERIOD}
            )
        )
    WHERE ROWNUM <= 50000
"""

cursor.execute(query)
result = cursor.fetchone()

if result:
    total, with_type, with_job = result
    print(f"Total active workers: {total:,}")
    print(f"With WORKER_TYPE_CODE: {with_type:,} ({with_type/total*100:.1f}%)")
    print(f"With JOB_CLASSIFICATION_CODE: {with_job:,} ({with_job/total*100:.1f}%)")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("MAPPING RECOMMENDATION")
print("=" * 80)
print("\nOption 1 - Department (Text 80):")
print("  Map: JOB_CLASSIFICATION_CODE description")
print("  Coverage: Check active worker percentage above")
print("  Note: Numeric codes (20, 29, 19) need description lookup")
print("\nOption 2 - DepartmentGroup (Picklist):")
print("  Map: WORKER_TYPE_CODE description")
print("  Coverage: ~100% (all workers have type code)")
print("  Values: 5 worker types (84% are type 02)")
print("  ISSUE: DepartmentGroup field does NOT exist in Salesforce")
