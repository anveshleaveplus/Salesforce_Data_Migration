"""
Diagnose Return Load Issues
Check Account matching and data quality
"""

import os
from dotenv import load_dotenv
import oracledb
from simple_salesforce import Salesforce

env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

print("="*70)
print("Return Load Diagnostic")
print("="*70)

# Connect to Oracle
print("\n[1/3] Checking Oracle Return data...")
conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

# Get unique employers from Returns
cursor.execute("""
    SELECT COUNT(DISTINCT EMPLOYER_ID) 
    FROM SCH_CO_20.CO_WSR
    WHERE PERIOD_END >= 202301
    AND EMPLOYER_ID != 23000
    AND ROWNUM <= 50000
""")
unique_employers = cursor.fetchone()[0]
print(f"      Unique Employers in Returns (50K sample): {unique_employers:,}")

# Get sample employer IDs
cursor.execute("""
    SELECT DISTINCT EMPLOYER_ID 
    FROM SCH_CO_20.CO_WSR
    WHERE PERIOD_END >= 202301
    AND EMPLOYER_ID != 23000
    AND ROWNUM <= 10
""")
sample_employer_ids = [str(row[0]) for row in cursor.fetchall()]
print(f"      Sample Employer IDs: {sample_employer_ids[:5]}")

cursor.close()
conn.close()

# Connect to Salesforce
print("\n[2/3] Checking Salesforce Accounts...")
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain=os.getenv('SF_DOMAIN', 'test')
)

# Total accounts in Salesforce
result = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null")
total_accounts = result['totalSize']
print(f"      Total Accounts with External_Id__c: {total_accounts:,}")

# Check if sample employers exist in Salesforce
ids_str = "','".join(sample_employer_ids[:5])
query = f"""
    SELECT External_Id__c 
    FROM Account 
    WHERE External_Id__c IN ('{ids_str}')
"""
result = sf.query(query)
found_accounts = [r['External_Id__c'] for r in result['records']]
print(f"      Sample Employers found in SF: {len(found_accounts)}/{len(sample_employer_ids[:5])}")
print(f"      Found: {found_accounts}")

not_found = set(sample_employer_ids[:5]) - set(found_accounts)
if not_found:
    print(f"      NOT FOUND: {list(not_found)}")

# Check Return__c object fields
print("\n[3/3] Checking Return__c object configuration...")
return_metadata = sf.Return__c.describe()

# Check required fields
required_fields = [f for f in return_metadata['fields'] if not f['nillable'] and not f['defaultedOnCreate']]
print(f"      Required fields:")
for field in required_fields:
    print(f"        - {field['name']} ({field['type']})")

# Check if Employer__c exists
employer_field = next((f for f in return_metadata['fields'] if f['name'] == 'Employer__c'), None)
if employer_field:
    print(f"\n      Employer__c field:")
    print(f"        Type: {employer_field['type']}")
    print(f"        Required: {not employer_field['nillable']}")
    print(f"        Reference To: {employer_field.get('referenceTo', [])}")
else:
    print(f"\n      [ERROR] Employer__c field not found!")

# Check External_Id__c
external_id_field = next((f for f in return_metadata['fields'] if f['name'] == 'External_Id__c'), None)
if external_id_field:
    print(f"\n      External_Id__c field:")
    print(f"        Type: {external_id_field['type']}")
    print(f"        Length: {external_id_field.get('length')}")
    print(f"        External ID: {external_id_field.get('externalId')}")
    print(f"        Unique: {external_id_field.get('unique')}")

# Check picklist fields
picklist_fields = ['ReturnType__c', 'InvoiceStatus__c']
for field_name in picklist_fields:
    field = next((f for f in return_metadata['fields'] if f['name'] == field_name), None)
    if field and field['type'] == 'picklist':
        values = [v['value'] for v in field.get('picklistValues', [])]
        print(f"\n      {field_name} picklist values:")
        for val in values[:10]:
            print(f"        - {val}")
        if len(values) > 10:
            print(f"        ... and {len(values) - 10} more")
    elif field:
        print(f"\n      {field_name}: {field['type']} (not a picklist)")
    else:
        print(f"\n      {field_name}: NOT FOUND")

print("\n" + "="*70)
print("DIAGNOSIS COMPLETE")
print("="*70)
print("""
Common Issues:
1. Employer__c required but Accounts not found → Load fails
2. Required fields not populated → Check required_fields above
3. Picklist values mismatch → Check picklist values above
4. Employer IDs in Returns but not in Accounts → Load more Accounts first
""")
