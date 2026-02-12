"""
Check total contact count and field officer coverage for ALL contacts
Compare: Current 50K active load vs loading ALL contacts
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
print("TOTAL CONTACT COUNT vs 50K ACTIVE LOAD")
print("=" * 80)

# [1] Total workers in system
print("\n[1] Total Workers in Oracle:")
print("-" * 80)

query = "SELECT COUNT(*) FROM CO_WORKER"
cursor.execute(query)
total_workers = cursor.fetchone()[0]
print(f"Total CO_WORKER records: {total_workers:,}")

# [2] Active workers (current 50K load criteria)
print("\n[2] Active Workers (Current Load Criteria):")
print("-" * 80)

ACTIVE_PERIOD = 202301

query = f"""
    SELECT COUNT(DISTINCT w.CUSTOMER_ID)
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
"""

cursor.execute(query)
active_workers = cursor.fetchone()[0]
print(f"Active workers (service >= 202301): {active_workers:,}")
print(f"Current load limit: 50,000")
print(f"If loaded all active: would load {active_workers:,} contacts")

# [3] Workers with employment history
print("\n[3] Workers with Employment History:")
print("-" * 80)

query = """
    SELECT COUNT(DISTINCT WORKER_ID)
    FROM CO_EMPLOYMENT_PERIOD
"""

cursor.execute(query)
workers_with_employment = cursor.fetchone()[0]
print(f"Workers with employment records: {workers_with_employment:,}")

# [4] Field officer coverage - ALL workers
print("\n[4] Field Officer Coverage - ALL Workers:")
print("-" * 80)

query = """
    SELECT 
        COUNT(DISTINCT w.CUSTOMER_ID) as total,
        COUNT(DISTINCT fv.CUSTOMER_ID) as with_field_visit,
        COUNT(DISTINCT CASE WHEN fov.ASSIGNED_TO IS NOT NULL THEN fv.CUSTOMER_ID END) as with_assigned_officer
    FROM CO_WORKER w
    LEFT JOIN CO_FIELD_VISIT_MEMBERS fvm ON w.CUSTOMER_ID = fvm.CUSTOMER_ID
    LEFT JOIN CO_FIELD_OFFICER_VISIT fov ON fvm.FIELD_OFFICER_VISIT_ID = fov.FIELD_OFFICER_VISIT_ID
    LEFT JOIN (SELECT DISTINCT CUSTOMER_ID FROM CO_FIELD_VISIT_MEMBERS) fv ON w.CUSTOMER_ID = fv.CUSTOMER_ID
"""

cursor.execute(query)
result = cursor.fetchone()

if result:
    total, with_visit, with_officer = result
    print(f"Total workers: {total:,}")
    print(f"With field visits: {with_visit:,} ({with_visit/total*100:.2f}%)")
    print(f"With assigned officer: {with_officer:,} ({with_officer/total*100:.2f}%)")

# [5] Field officer assignments - breakdown
print("\n[5] Field Officer Assignments - Detailed:")
print("-" * 80)

query = """
    SELECT 
        fov.ASSIGNED_TO,
        COUNT(DISTINCT fvm.CUSTOMER_ID) as worker_count
    FROM CO_FIELD_VISIT_MEMBERS fvm
    JOIN CO_FIELD_OFFICER_VISIT fov ON fvm.FIELD_OFFICER_VISIT_ID = fov.FIELD_OFFICER_VISIT_ID
    WHERE fov.ASSIGNED_TO IS NOT NULL
    GROUP BY fov.ASSIGNED_TO
    ORDER BY COUNT(DISTINCT fvm.CUSTOMER_ID) DESC
    FETCH FIRST 20 ROWS ONLY
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Top 20 field officers by worker count:")
    print(f"{'Officer':<20} {'Workers':>12}")
    print("-" * 34)
    total_assignments = 0
    for officer, count in results:
        print(f"{str(officer):<20} {count:>12,}")
        total_assignments += count
    print("-" * 34)
    print(f"{'Total (top 20)':<20} {total_assignments:>12,}")

# [6] LSL coverage - ALL workers
print("\n[6] LSL Coverage - ALL Workers:")
print("-" * 80)

query = """
    SELECT 
        COUNT(*) as total,
        COUNT(ent.LSL_DUE) as with_lsl,
        COUNT(CASE WHEN ent.LSL_DUE > 0 THEN 1 END) as with_positive_lsl
    FROM CO_WORKER w
    LEFT JOIN CO_WORKER_ENTITLEMENT_V4 ent ON w.CUSTOMER_ID = ent.CUSTOMER_ID
"""

cursor.execute(query)
result = cursor.fetchone()

if result:
    total, with_lsl, with_positive = result
    print(f"Total workers: {total:,}")
    print(f"With LSL_DUE: {with_lsl:,} ({with_lsl/total*100:.2f}%)")
    print(f"With positive LSL: {with_positive:,} ({with_positive/total*100:.2f}%)")

# [7] Breakdown by worker status
print("\n[7] Worker Status Breakdown:")
print("-" * 80)

query = """
    SELECT 
        WORKER_STATUS_ID,
        COUNT(*) as count
    FROM CO_WORKER
    GROUP BY WORKER_STATUS_ID
    ORDER BY COUNT(*) DESC
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"{'Status ID':<15} {'Count':>12} {'%':>8}")
    print("-" * 37)
    for status_id, count in results:
        pct = (count / total_workers * 100) if total_workers > 0 else 0
        print(f"{str(status_id):<15} {count:>12,} {pct:>7.2f}%")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("RECOMMENDATION")
print("=" * 80)
print(f"\nCurrent load: 50,000 active workers (limit)")
print(f"Total workers in system: {total_workers:,}")
print(f"\nIf you load ALL contacts:")
print(f"  - {total_workers:,} total contacts")
print(f"  - Field officer coverage may be higher (check percentage above)")
print(f"  - LSL coverage shown above")
print(f"  - Includes inactive/historical workers")
print(f"\nDecision: Load all {total_workers:,} or keep 50K limit for active workers?")
