"""
Analyze field officer ASSIGNED_TO values for User lookup mapping
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
print("FIELD OFFICER USER LOOKUP ANALYSIS")
print("="*80)
print("Salesforce Field: Contact.FieldOfficerAllocated__c (Lookup to User)")
print("="*80)

# 1. Sample ASSIGNED_TO values
print("\n[1] Sample Field Officer ASSIGNED_TO values:")
print("-"*80)

sample_query = """
SELECT DISTINCT 
    fov.ASSIGNED_TO,
    COUNT(*) as visit_count
FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT fov
WHERE fov.ASSIGNED_TO IS NOT NULL
GROUP BY fov.ASSIGNED_TO
ORDER BY COUNT(*) DESC
"""

cursor.execute(sample_query)
assigned_to_samples = cursor.fetchall()

print(f"{'ASSIGNED_TO (User)':<30} {'VISIT_COUNT':<15}")
print("-"*80)
for row in assigned_to_samples[:20]:  # Show top 20
    print(f"{str(row[0]):<30} {row[1]:<15,}")

print(f"\nTotal unique field officers: {len(assigned_to_samples)}")

# 2. Check format of ASSIGNED_TO (username, employee ID, etc.)
print("\n[2] ASSIGNED_TO Value Format Analysis:")
print("-"*80)

if assigned_to_samples:
    sample_value = assigned_to_samples[0][0]
    print(f"Sample value: {sample_value}")
    print(f"Length: {len(sample_value)}")
    print(f"Format: {'Numeric' if sample_value.isdigit() else 'Text/Username'}")

# 3. Workers with field officer assignments (most recent)
print("\n[3] Active Workers with Most Recent Field Officer Assignment:")
print("-"*80)

worker_fo_query = """
WITH latest_visits AS (
    SELECT 
        fvm.CUSTOMER_ID,
        fov.ASSIGNED_TO,
        fov.CREATED_WHEN,
        ROW_NUMBER() OVER (PARTITION BY fvm.CUSTOMER_ID ORDER BY fov.CREATED_WHEN DESC NULLS LAST) as rn
    FROM SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm
    INNER JOIN SCH_CO_20.CO_FIELD_OFFICER_VISIT fov 
        ON fvm.FIELD_OFFICER_VISIT_ID = fov.FIELD_OFFICER_VISIT_ID
    WHERE fov.ASSIGNED_TO IS NOT NULL
)
SELECT 
    CUSTOMER_ID,
    ASSIGNED_TO,
    CREATED_WHEN
FROM latest_visits
WHERE rn = 1
AND ROWNUM <= 20
ORDER BY CUSTOMER_ID
"""

cursor.execute(worker_fo_query)
worker_fo = cursor.fetchall()

print(f"{'WORKER_ID':<15} {'ASSIGNED_TO_USER':<30} {'LAST_VISIT_DATE':<20}")
print("-"*80)
for row in worker_fo:
    visit_date = row[2].strftime('%Y-%m-%d %H:%M:%S') if row[2] else 'NULL'
    print(f"{str(row[0]):<15} {str(row[1]):<30} {visit_date:<20}")

# 4. Coverage for active workers
print("\n[4] Coverage Analysis for Active Workers:")
print("-"*80)

coverage_query = """
SELECT 
    COUNT(DISTINCT w.CUSTOMER_ID) as total_workers,
    COUNT(DISTINCT CASE WHEN fvm.CUSTOMER_ID IS NOT NULL THEN w.CUSTOMER_ID END) as with_field_officer,
    COUNT(DISTINCT CASE WHEN fov.ASSIGNED_TO IS NOT NULL THEN w.CUSTOMER_ID END) as with_assigned_officer
FROM (
    SELECT CUSTOMER_ID
    FROM SCH_CO_20.CO_WORKER
    WHERE WORKER_STATUS_ID = 10
) w
LEFT JOIN SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm ON w.CUSTOMER_ID = fvm.CUSTOMER_ID
LEFT JOIN SCH_CO_20.CO_FIELD_OFFICER_VISIT fov ON fvm.FIELD_OFFICER_VISIT_ID = fov.FIELD_OFFICER_VISIT_ID
"""

cursor.execute(coverage_query)
coverage = cursor.fetchone()

total = coverage[0]
with_fo = coverage[1]
with_assigned = coverage[2]

print(f"Total Active Workers:              {total:,}")
print(f"Workers with Field Visit:          {with_fo:,} ({with_fo/total*100:.2f}%)")
print(f"Workers with Assigned Officer:     {with_assigned:,} ({with_assigned/total*100:.2f}%)")

print("\n" + "="*80)
print("MAPPING IMPLEMENTATION:")
print("="*80)
print("Oracle Source:")
print("  Table: CO_FIELD_OFFICER_VISIT")
print("  Field: ASSIGNED_TO (VARCHAR2 30)")
print("  Via: CO_FIELD_VISIT_MEMBERS.CUSTOMER_ID linkage")
print("  Logic: Get most recent field officer visit per worker")
print()
print("Salesforce Target:")
print("  Field: Contact.FieldOfficerAllocated__c")
print("  Type: Lookup(User)")
print()
print("Implementation Steps:")
print("  1. LEFT JOIN CO_FIELD_VISIT_MEMBERS on CUSTOMER_ID")
print("  2. Get most recent FIELD_OFFICER_VISIT_ID per worker (ORDER BY CREATED_WHEN DESC)")
print("  3. Get ASSIGNED_TO from CO_FIELD_OFFICER_VISIT")
print("  4. In Python: Query Salesforce Users to match ASSIGNED_TO")
print("     - Try matching: User.Username = ASSIGNED_TO")
print("     - OR: User.EmployeeNumber = ASSIGNED_TO")
print("     - Store User.Id for lookup")
print()
print("⚠️  Challenges:")
print("  - Need to verify ASSIGNED_TO format matches SF User.Username or EmployeeNumber")
print("  - May have unmapped users if Oracle users don't exist in Salesforce")
print("  - Should handle multiple visits per worker (use most recent)")
print("="*80)

cursor.close()
conn.close()
