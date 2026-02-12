"""
Investigate field officer tables to determine if worker has field officer allocated
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
print("FIELD OFFICER ALLOCATION ANALYSIS")
print("="*80)

# 1. Check CO_FIELD_OFFICER table structure
print("\n[1] CO_FIELD_OFFICER table structure:")
print("-"*80)

fo_struct = """
SELECT column_name, data_type, data_length, nullable
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND table_name = 'CO_FIELD_OFFICER'
ORDER BY column_name
"""

cursor.execute(fo_struct)
fo_cols = cursor.fetchall()

print(f"{'COLUMN_NAME':<35} {'TYPE':<15} {'LENGTH':<10} {'NULLABLE':<10}")
print("-"*80)
for row in fo_cols:
    print(f"{row[0]:<35} {row[1]:<15} {str(row[2]):<10} {row[3]:<10}")

# 2. Check CO_FIELD_VISIT_MEMBERS table structure
print("\n[2] CO_FIELD_VISIT_MEMBERS table structure:")
print("-"*80)

fvm_struct = """
SELECT column_name, data_type, data_length, nullable
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND table_name = 'CO_FIELD_VISIT_MEMBERS'
ORDER BY column_name
"""

cursor.execute(fvm_struct)
fvm_cols = cursor.fetchall()

print(f"{'COLUMN_NAME':<35} {'TYPE':<15} {'LENGTH':<10} {'NULLABLE':<10}")
print("-"*80)
for row in fvm_cols:
    print(f"{row[0]:<35} {row[1]:<15} {str(row[2]):<10} {row[3]:<10}")

# 3. Check CO_FIELD_OFFICER_VISIT table structure
print("\n[3] CO_FIELD_OFFICER_VISIT table structure:")
print("-"*80)

fov_struct = """
SELECT column_name, data_type, data_length, nullable
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND table_name = 'CO_FIELD_OFFICER_VISIT'
ORDER BY column_name
"""

cursor.execute(fov_struct)
fov_cols = cursor.fetchall()

print(f"{'COLUMN_NAME':<35} {'TYPE':<15} {'LENGTH':<10} {'NULLABLE':<10}")
print("-"*80)
for row in fov_cols:
    print(f"{row[0]:<35} {row[1]:<15} {str(row[2]):<10} {row[3]:<10}")

# 4. Sample data - check if we can link workers to field officers
print("\n[4] Sample Field Visit Members data (10 records):")
print("-"*80)

sample_query = """
SELECT 
    fvm.FIELD_VISIT_MEMBERS_ID,
    fvm.FIELD_OFFICER_VISIT_ID,
    fvm.CUSTOMER_ID as WORKER_CUSTOMER_ID
FROM SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm
WHERE ROWNUM <= 10
ORDER BY fvm.FIELD_VISIT_MEMBERS_ID
"""

cursor.execute(sample_query)
samples = cursor.fetchall()

print(f"{'MEMBERS_ID':<15} {'VISIT_ID':<15} {'WORKER_ID':<15}")
print("-"*80)
for row in samples:
    print(f"{str(row[0]):<15} {str(row[1]):<15} {str(row[2]):<15}")

# 5. Check if active workers have field officer visits
print("\n[5] Active Workers with Field Officer Visits:")
print("-"*80)

active_query = """
SELECT 
    COUNT(DISTINCT w.CUSTOMER_ID) as total_active_workers,
    COUNT(DISTINCT fvm.CUSTOMER_ID) as workers_with_field_visits,
    ROUND(COUNT(DISTINCT fvm.CUSTOMER_ID) * 100.0 / COUNT(DISTINCT w.CUSTOMER_ID), 2) as percentage
FROM (
    SELECT CUSTOMER_ID
    FROM SCH_CO_20.CO_WORKER
    WHERE WORKER_STATUS_ID = 10
) w
LEFT JOIN SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm ON w.CUSTOMER_ID = fvm.CUSTOMER_ID
"""

cursor.execute(active_query)
coverage = cursor.fetchone()

print(f"Total Active Workers:           {coverage[0]:,}")
print(f"Workers with Field Visits:      {coverage[1]:,}")
print(f"Coverage:                       {coverage[2]}%")

# 6. Sample join - workers with field officer allocation
print("\n[6] Sample Workers with Field Officer Details (10 records):")
print("-"*80)

join_query = """
SELECT 
    w.CUSTOMER_ID,
    fov.FIELD_OFFICER_VISIT_ID,
    fov.ASSIGNED_TO as FIELD_OFFICER_ID,
    fov.FIELD_VISIT_DATE
FROM (
    SELECT CUSTOMER_ID
    FROM SCH_CO_20.CO_WORKER
    WHERE WORKER_STATUS_ID = 10
    AND ROWNUM <= 100
) w
INNER JOIN SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm ON w.CUSTOMER_ID = fvm.CUSTOMER_ID
INNER JOIN SCH_CO_20.CO_FIELD_OFFICER_VISIT fov ON fvm.FIELD_OFFICER_VISIT_ID = fov.FIELD_OFFICER_VISIT_ID
WHERE ROWNUM <= 10
"""

cursor.execute(join_query)
join_samples = cursor.fetchall()

print(f"{'WORKER_ID':<15} {'VISIT_ID':<15} {'OFFICER_ID':<20} {'VISIT_DATE':<15}")
print("-"*80)
for row in join_samples:
    visit_date = row[3].strftime('%Y-%m-%d') if row[3] else 'NULL'
    print(f"{str(row[0]):<15} {str(row[1]):<15} {str(row[2]):<20} {visit_date:<15}")

print("\n" + "="*80)
print("MAPPING RECOMMENDATION:")
print("="*80)
print("Salesforce: Contact.FieldOfficerAllocated__c")
print()
print("Option 1 - Boolean (Has Field Officer Visit):")
print("  Logic: TRUE if worker exists in CO_FIELD_VISIT_MEMBERS, FALSE otherwise")
print("  Query: EXISTS (SELECT 1 FROM CO_FIELD_VISIT_MEMBERS WHERE CUSTOMER_ID = worker)")
print()
print("Option 2 - Lookup to Field Officer (if SF has Field Officer object):")
print("  Source: CO_FIELD_OFFICER_VISIT.ASSIGNED_TO via CO_FIELD_VISIT_MEMBERS")
print("  Requires: Field Officer lookup object in Salesforce")
print()
print("📌 Need to check Salesforce field type for FieldOfficerAllocated__c")
print("="*80)

cursor.close()
conn.close()
