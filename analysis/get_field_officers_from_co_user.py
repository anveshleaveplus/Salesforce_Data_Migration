"""
Get Field Officer details from CO_USER table
Based on Oracle Admin guidance: Field officers are staff in CO_USER, not members in CO_WORKER
"""

import os
from dotenv import load_dotenv
import oracledb
import csv

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
print("FIELD OFFICER DETAILS FROM CO_USER TABLE")
print("=" * 80)

# [1] First check CO_USER table structure
print("\n[1] CO_USER Table Structure:")
print("-" * 80)

query = """
    SELECT column_name, data_type, nullable
    FROM all_tab_columns
    WHERE owner = 'SCH_CO_20'
        AND table_name = 'CO_USER'
    ORDER BY column_id
"""

cursor.execute(query)
results = cursor.fetchall()

for row in results:
    print(f"  {row[0]:<30} {row[1]:<15} {'NULL' if row[2] == 'Y' else 'NOT NULL'}")

# [2] Get Field Officer details from CO_USER
print("\n[2] Field Officers from CO_USER:")
print("-" * 80)

query = """
    SELECT 
        u.USERID,
        u.USER_NAME,
        u.IS_ACTIVE,
        u.JOB_TITLE,
        u.MOBILE_PHONE_NO,
        COUNT(DISTINCT fvm.CUSTOMER_ID) as workers_assigned
    FROM SCH_CO_20.CO_USER u
    LEFT JOIN SCH_CO_20.CO_FIELD_OFFICER_VISIT fov 
        ON u.USERID = fov.ASSIGNED_TO
    LEFT JOIN SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm 
        ON fov.FIELD_OFFICER_VISIT_ID = fvm.FIELD_OFFICER_VISIT_ID
    LEFT JOIN SCH_CO_20.CO_WORKER w 
        ON fvm.CUSTOMER_ID = w.CUSTOMER_ID
    LEFT JOIN SCH_CO_20.CO_SERVICE s 
        ON s.WORKER = w.CUSTOMER_ID
    WHERE u.USERID IN (
        SELECT DISTINCT ASSIGNED_TO
        FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT
        WHERE ASSIGNED_TO IS NOT NULL
    )
    AND (s.PERIOD_END >= 202301 OR s.PERIOD_END IS NULL OR s.PERIOD_END IS NULL)
    GROUP BY u.USERID, u.USER_NAME, u.IS_ACTIVE, u.JOB_TITLE, u.MOBILE_PHONE_NO
    ORDER BY workers_assigned DESC, u.USERID
"""

cursor.execute(query)
results = cursor.fetchall()

print(f"\nFound {len(results)} Field Officers:\n")
print(f"{'User ID':<15} {'Name':<30} {'Active':<10} {'Job Title':<25} {'Workers'}")
print("-" * 90)

active_count = 0
inactive_count = 0

for row in results:
    user_id = row[0] if row[0] else "N/A"
    name = row[1] if row[1] else "N/A"
    is_active = "✓ Yes" if row[2] == 'Y' else "✗ No"
    job_title = row[3] if row[3] else "N/A"
    workers = row[5] if row[5] else 0
    
    if row[2] == 'Y':
        active_count += 1
    else:
        inactive_count += 1
    
    print(f"{user_id:<15} {name:<30} {is_active:<10} {job_title:<25} {workers}")

print("-" * 90)
print(f"Active: {active_count} | Inactive: {inactive_count} | Total: {len(results)}")

# [3] Export to CSV for Salesforce user creation
print("\n[3] Exporting to CSV:")
print("-" * 80)

cursor.execute(query)
results = cursor.fetchall()

csv_file = "field_officers_from_oracle.csv"
with open(csv_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['Officer_Code', 'User_Name', 'Is_Active', 'Job_Title', 'Mobile_Phone', 'Workers_Assigned'])
    
    for row in results:
        writer.writerow([
            row[0],  # USERID
            row[1],  # USER_NAME
            row[2],  # IS_ACTIVE
            row[3],  # JOB_TITLE
            row[4],  # MOBILE_PHONE_NO
            row[5]   # workers_assigned
        ])

print(f"✅ Exported: {csv_file}")
print(f"   Total records: {len(results)}")

# [4] Check for missing officers
print("\n[4] Checking for officers NOT in CO_USER:")
print("-" * 80)

query = """
    SELECT DISTINCT ASSIGNED_TO
    FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT
    WHERE ASSIGNED_TO IS NOT NULL
        AND ASSIGNED_TO NOT IN (
            SELECT USERID FROM SCH_CO_20.CO_USER
        )
    ORDER BY ASSIGNED_TO
"""

cursor.execute(query)
missing = cursor.fetchall()

if missing:
    print(f"⚠️  Found {len(missing)} officer codes NOT in CO_USER:")
    for row in missing:
        print(f"  {row[0]}")
else:
    print("✅ All officer codes found in CO_USER!")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Field Officers found in CO_USER: {len(results)}")
print(f"   - Active: {active_count}")
print(f"   - Inactive: {inactive_count}")
print(f"📁 CSV exported: {csv_file}")
print(f"\nNext steps:")
print(f"  1. Review CSV for active officers")
print(f"  2. Send to SF Admin to create user accounts")
print(f"  3. Map USER_ID → Salesforce Username")
print("=" * 80)
