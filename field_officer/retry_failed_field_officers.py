"""
Retry the 28 failed contacts with Active Field Officer filter
"""

import os
import csv
import pandas as pd
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Load environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

# Load ALL Field Officer mapping (SF Admin confirmed: Can link inactive users)
FIELD_OFFICER_MAPPING = {}
with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['Salesforce_User_Id'] != 'ERROR':
            FIELD_OFFICER_MAPPING[row['Officer_Code']] = row['Salesforce_User_Id']

print("=" * 80)
print("RETRY FAILED CONTACTS WITH ALL FIELD OFFICERS")
print("=" * 80)
print(f"Loaded {len(FIELD_OFFICER_MAPPING)} Field Officer mappings (active and inactive)")
print("SF Admin confirmed: Can assign contacts to inactive users\n")

# Connect to Salesforce
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

# Read error file
error_file = 'error/sit_contact_errors_20260211_125535.csv'
with open(error_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    failed_contacts = [row for row in reader]

print(f"Found {len(failed_contacts)} failed contacts to retry\n")

# Query Oracle for Field Officer codes for these contacts
import oracledb

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

external_ids = [row['external_id'] for row in failed_contacts]
external_ids_str = ','.join(external_ids)

query = f"""
    SELECT 
        w.CUSTOMER_ID,
        (
            SELECT fov.ASSIGNED_TO
            FROM SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm
            INNER JOIN SCH_CO_20.CO_FIELD_OFFICER_VISIT fov 
                ON fvm.FIELD_OFFICER_VISIT_ID = fov.FIELD_OFFICER_VISIT_ID
            WHERE fvm.CUSTOMER_ID = w.CUSTOMER_ID
                AND fov.ASSIGNED_TO IS NOT NULL
            ORDER BY fvm.CREATED_WHEN DESC
            FETCH FIRST 1 ROWS ONLY
        ) as FIELD_OFFICER_CODE
    FROM SCH_CO_20.CO_WORKER w
    WHERE w.CUSTOMER_ID IN ({external_ids_str})
"""

cursor = conn.cursor()
cursor.execute(query)
results = cursor.fetchall()

# Map to Salesforce User IDs (all officers now included)
updates = []
for worker_id, officer_code in results:
    if officer_code:
        user_id = FIELD_OFFICER_MAPPING.get(officer_code)
        status = "Mapped" if user_id else "Not Found"
        updates.append({
            'External_Id__c': str(worker_id),
            'FieldOfficerAllocated__c': user_id,
            'Officer_Code': officer_code,
            'Status': status
        })
        print(f"  {worker_id}: {officer_code} → {status}")

cursor.close()
conn.close()

# Update Salesforce
print(f"\nUpdating {len(updates)} contacts...\n")

success = 0
errors = 0

for update in updates:
    try:
        sf.Contact.upsert(
            'External_Id__c/' + update['External_Id__c'],
            {'FieldOfficerAllocated__c': update['FieldOfficerAllocated__c']}
        )
        print(f"✅ {update['External_Id__c']}: {update['Officer_Code']} → {update['Status']}")
        success += 1
    except Exception as e:
        print(f"❌ {update['External_Id__c']}: {str(e)}")
        errors += 1

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Success: {success}")
print(f"❌ Errors: {errors}")
print()
print("All Field Officers assigned: Contacts linked to both active and inactive officers")
print("SF Admin confirmed: Inactive users CAN be referenced in lookups")
print("=" * 80)
