"""
Export 47 field officer codes to CSV for Field Operations Manager
"""

import os
import csv
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
print("EXPORTING FIELD OFFICER LIST")
print("=" * 80)

# Get all field officers with assignment counts
query = """
    SELECT 
        ASSIGNED_TO,
        COUNT(DISTINCT fvm.CUSTOMER_ID) as worker_count
    FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT fov
    JOIN SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm 
        ON fov.FIELD_OFFICER_VISIT_ID = fvm.FIELD_OFFICER_VISIT_ID
    WHERE ASSIGNED_TO IS NOT NULL
    GROUP BY ASSIGNED_TO
    ORDER BY COUNT(DISTINCT fvm.CUSTOMER_ID) DESC
"""

cursor.execute(query)
results = cursor.fetchall()

# Export to CSV
output_file = 'field_officers_list.csv'

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    
    # Header
    writer.writerow([
        'Officer_Code',
        'Workers_Assigned',
        'Full_Name',
        'Email_Address',
        'Notes'
    ])
    
    # Data rows
    for officer_code, worker_count in results:
        writer.writerow([
            officer_code,
            worker_count,
            '',  # Full_Name - to be filled by Field Ops Manager
            '',  # Email_Address - to be filled by Field Ops Manager
            ''   # Notes - optional
        ])

print(f"\n[OK] Exported {len(results)} field officers to: {output_file}")
print(f"\nTotal worker assignments: {sum(count for _, count in results):,}")
print(f"\nNext steps:")
print(f"  1. Send {output_file} to Field Operations Manager")
print(f"  2. Ask them to fill in Full_Name and Email_Address columns")
print(f"  3. Return completed CSV to SF Admin for user creation")

cursor.close()
conn.close()
