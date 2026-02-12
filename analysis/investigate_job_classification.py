"""
Investigate JOB_CLASSIFICATION_CODE and WORKER_TYPE_CODE for Department mapping
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
print("JOB CLASSIFICATION AND WORKER TYPE INVESTIGATION")
print("=" * 80)

# [1] CO_EMPLOYMENT_PERIOD.JOB_CLASSIFICATION_CODE
print("\n[1] CO_EMPLOYMENT_PERIOD.JOB_CLASSIFICATION_CODE Values:")
print("-" * 80)

query = """
    SELECT JOB_CLASSIFICATION_CODE, COUNT(*) as cnt
    FROM CO_EMPLOYMENT_PERIOD
    WHERE JOB_CLASSIFICATION_CODE IS NOT NULL
    GROUP BY JOB_CLASSIFICATION_CODE
    ORDER BY COUNT(*) DESC
    FETCH FIRST 20 ROWS ONLY
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    total = sum(row[1] for row in results)
    print(f"Found {len(results)} unique job classification codes (from {total:,} records):")
    print(f"{'Code':<20} {'Count':>12} {'%':>8}")
    print("-" * 42)
    for code, count in results:
        pct = (count / total * 100) if total > 0 else 0
        print(f"{str(code):<20} {count:>12,} {pct:>7.2f}%")
else:
    print("No JOB_CLASSIFICATION_CODE values found")

# Check NULL count
query_null = "SELECT COUNT(*) FROM CO_EMPLOYMENT_PERIOD WHERE JOB_CLASSIFICATION_CODE IS NULL"
cursor.execute(query_null)
null_count = cursor.fetchone()[0]
print(f"\nNULL JOB_CLASSIFICATION_CODE: {null_count:,}")

# [2] Check if there's a code set for JOB_CLASSIFICATION_CODE
print("\n[2] Code Set for JOB_CLASSIFICATION_CODE:")
print("-" * 80)

query = """
    SELECT CODE_SET_ID, CODE_SET_NAME
    FROM CO_CODE_SET
    WHERE UPPER(CODE_SET_NAME) LIKE '%JOB%'
       OR UPPER(CODE_SET_NAME) LIKE '%CLASSIF%'
    ORDER BY CODE_SET_ID
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} job/classification code sets:")
    for code_set_id, name in results:
        print(f"  Code Set {code_set_id}: {name}")
else:
    print("No job classification code sets found")

# [3] CO_WORKER.WORKER_TYPE_CODE
print("\n[3] CO_WORKER.WORKER_TYPE_CODE Values:")
print("-" * 80)

query = """
    SELECT WORKER_TYPE_CODE, COUNT(*) as cnt
    FROM CO_WORKER
    WHERE WORKER_TYPE_CODE IS NOT NULL
    GROUP BY WORKER_TYPE_CODE
    ORDER BY COUNT(*) DESC
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    total = sum(row[1] for row in results)
    print(f"Found {len(results)} unique worker type codes (from {total:,} workers):")
    print(f"{'Code':<20} {'Count':>12} {'%':>8}")
    print("-" * 42)
    for code, count in results:
        pct = (count / total * 100) if total > 0 else 0
        print(f"{str(code):<20} {count:>12,} {pct:>7.2f}%")
else:
    print("No WORKER_TYPE_CODE values found")

# Check NULL count
query_null = "SELECT COUNT(*) FROM CO_WORKER WHERE WORKER_TYPE_CODE IS NULL"
cursor.execute(query_null)
null_count = cursor.fetchone()[0]
print(f"\nNULL WORKER_TYPE_CODE: {null_count:,}")

# [4] Check if there's a code set for WORKER_TYPE_CODE
print("\n[4] Code Set for WORKER_TYPE_CODE:")
print("-" * 80)

query = """
    SELECT CODE_SET_ID, CODE_SET_NAME
    FROM CO_CODE_SET
    WHERE UPPER(CODE_SET_NAME) LIKE '%WORKER%TYPE%'
       OR UPPER(CODE_SET_NAME) LIKE '%TYPE%'
       OR CODE_SET_NAME = 'WorkerTypeCode'
    ORDER BY CODE_SET_ID
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} worker type code sets:")
    for code_set_id, name in results:
        print(f"  Code Set {code_set_id}: {name}")
else:
    print("No worker type code sets found")

# [5] Sample active workers with these values
print("\n[5] Sample Active Workers with Job/Type Codes:")
print("-" * 80)

query = """
    SELECT 
        w.CUSTOMER_ID,
        w.WORKER_TYPE_CODE,
        ep.JOB_CLASSIFICATION_CODE,
        p.FIRST_NAME,
        p.SURNAME
    FROM CO_WORKER w
    JOIN CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
    LEFT JOIN CO_EMPLOYMENT_PERIOD ep 
        ON w.CUSTOMER_ID = ep.WORKER_ID 
        AND ep.EFFECTIVE_TO_DATE IS NULL
    WHERE ROWNUM <= 20
"""

cursor.execute(query)
results = cursor.fetchall()

print(f"{'CUSTOMER_ID':<12} {'WORKER_TYPE':<15} {'JOB_CLASS':<15} {'Name':<30}")
print("-" * 74)
for cust_id, worker_type, job_class, first, last in results:
    name = f"{first or ''} {last or ''}".strip()
    w_type = str(worker_type) if worker_type else 'NULL'
    j_class = str(job_class) if job_class else 'NULL'
    print(f"{cust_id:<12} {w_type:<15} {j_class:<15} {name:<30}")

# [6] CO_CUSTOMER.ROLE_ID investigation
print("\n[6] CO_CUSTOMER.ROLE_ID Values:")
print("-" * 80)

query = """
    SELECT ROLE_ID, COUNT(*) as cnt
    FROM CO_CUSTOMER
    WHERE ROLE_ID IS NOT NULL
    GROUP BY ROLE_ID
    ORDER BY COUNT(*) DESC
    FETCH FIRST 20 ROWS ONLY
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    total = sum(row[1] for row in results)
    print(f"Found {len(results)} unique role IDs:")
    for role_id, count in results:
        print(f"  ROLE_ID {role_id}: {count:,} customers")
else:
    print("No ROLE_ID values found")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print("\nPotential Mappings:")
print("  Contact.Department (Text 80)")
print("    → CO_EMPLOYMENT_PERIOD.JOB_CLASSIFICATION_CODE")
print("    → OR CO_WORKER.WORKER_TYPE_CODE")
print("\n  Contact.DepartmentGroup (Picklist)")
print("    → Field does NOT exist in Salesforce")
print("    → May need to be created first")
