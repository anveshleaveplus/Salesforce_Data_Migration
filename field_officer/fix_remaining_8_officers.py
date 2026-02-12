"""
Fix the 8 remaining Field Officer users that failed during bulk creation
"""
import os
import csv
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Load SIT environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("=" * 80)
print("FIXING 8 REMAINING FIELD OFFICERS")
print("=" * 80)

# Get Standard User profile
profile = sf.query("SELECT Id FROM Profile WHERE Name = 'Standard User' LIMIT 1")['records'][0]

# Load current mapping to check for existing users
existing_mapping = {}
try:
    with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_mapping[row['Officer_Code']] = row
except FileNotFoundError:
    pass

# 8 officers that failed
failed_officers = [
    # Duplicates - try with sit3 suffix
    {'code': 'ANDREW.MACKENZIE', 'first': 'Andrew', 'last': 'Mackenzie', 'email': 'andrew.mackenzie.sit3@leaveplus.com.au', 'active': 'N'},
    {'code': 'MATT.MCPHEE', 'first': 'Matt', 'last': 'McPhee', 'email': 'matt.mcphee.sit3@leaveplus.com.au', 'active': 'N'},
    {'code': 'MICHAEL.DOCHERTY', 'first': 'Michael', 'last': 'Docherty', 'email': 'michael.docherty.sit3@leaveplus.com.au', 'active': 'N'},
    {'code': 'AM1', 'first': 'Andrew', 'last': 'Mathers', 'email': 'andrew.mathers.sit3@leaveplus.com.au', 'active': 'N'},
    {'code': 'JT', 'first': 'Jeremy', 'last': 'Tobin', 'email': 'jeremy.tobin.sit3@leaveplus.com.au', 'active': 'N'},
    {'code': 'MRD', 'first': 'Matthew', 'last': 'Ridgwell-Daly', 'email': 'matthew.ridgwell.sit3@leaveplus.com.au', 'active': 'N'},
    
    # Apostrophes - remove from email
    {'code': 'JJO', 'first': 'John', 'last': "O'Dea", 'email': 'john.odea.sit@leaveplus.com.au', 'active': 'N'},
    {'code': 'MOB', 'first': 'Michael', 'last': "O'Brien", 'email': 'michael.obrien.sit@leaveplus.com.au', 'active': 'N'},
]

results = []

for officer in failed_officers:
    code = officer['code']
    username = officer['email']
    
    print(f"\n{code} ({officer['first']} {officer['last']})")
    print("-" * 80)
    
    # Check if already exists in mapping
    if code in existing_mapping:
        user_id = existing_mapping[code]['Salesforce_User_Id']
        if user_id and user_id != 'ERROR':
            print(f"  ✅ Already in mapping: {user_id}")
            results.append({
                'code': code,
                'user_id': user_id,
                'email': officer['email'],
                'status': 'Already Mapped',
                'active': officer['active']
            })
            continue
    
    # Check if user exists by username
    try:
        # Escape apostrophes in SOQL query
        escaped_username = username.replace("'", "\\'")
        query = f"SELECT Id FROM User WHERE Username = '{escaped_username}'"
        existing = sf.query(query)
        
        if existing['totalSize'] > 0:
            user_id = existing['records'][0]['Id']
            print(f"  ✅ Found existing user: {user_id}")
            results.append({
                'code': code,
                'user_id': user_id,
                'email': username,
                'status': 'Already Existed',
                'active': officer['active']
            })
            continue
    except Exception as e:
        print(f"  ⚠️  Query check failed: {str(e)[:80]}")
    
    # Create new user
    try:
        user_data = {
            'Username': username,
            'Email': username,
            'FirstName': officer['first'],
            'LastName': officer['last'],
            'Alias': code[:8] if len(code) <= 8 else code[:4] + str(hash(code))[-4:],
            'ProfileId': profile['Id'],
            'TimeZoneSidKey': 'Australia/Sydney',
            'LocaleSidKey': 'en_AU',
            'EmailEncodingKey': 'UTF-8',
            'LanguageLocaleKey': 'en_US',
            'IsActive': True  # Create as active, can deactivate later if needed
        }
        
        result = sf.User.create(user_data)
        user_id = result['id']
        print(f"  ✅ Created: {user_id}")
        
        results.append({
            'code': code,
            'user_id': user_id,
            'email': username,
            'status': 'Created',
            'active': officer['active']
        })
        
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Failed: {error_msg[:100]}")
        results.append({
            'code': code,
            'user_id': 'ERROR',
            'email': username,
            'status': f'Error: {error_msg[:50]}',
            'active': officer['active']
        })

# Update mapping file
print("\n" + "=" * 80)
print("UPDATING MAPPING FILE")
print("=" * 80)

# Read existing mapping
mapping_rows = []
if existing_mapping:
    mapping_rows = list(existing_mapping.values())

# Update with new results
for result in results:
    # Find and update existing row, or add new
    found = False
    for row in mapping_rows:
        if row['Officer_Code'] == result['code']:
            row['Salesforce_User_Id'] = result['user_id']
            row['Email'] = result['email']
            row['Status'] = result['status']
            row['Is_Active'] = result['active']
            found = True
            break
    
    if not found:
        mapping_rows.append({
            'Officer_Code': result['code'],
            'Salesforce_User_Id': result['user_id'],
            'Email': result['email'],
            'Status': result['status'],
            'Is_Active': result['active']
        })

# Write updated mapping
with open('field_officer_salesforce_mapping.csv', 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['Officer_Code', 'Salesforce_User_Id', 'Email', 'Status', 'Is_Active']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(mapping_rows)

print(f"✅ Updated field_officer_salesforce_mapping.csv with {len(mapping_rows)} officers")

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

created = sum(1 for r in results if r['status'] == 'Created')
existed = sum(1 for r in results if r['status'] in ['Already Existed', 'Already Mapped'])
errors = sum(1 for r in results if 'ERROR' in r['user_id'])

print(f"✅ Created: {created}")
print(f"✅ Already Existed: {existed}")
print(f"❌ Errors: {errors}")

if errors == 0:
    print("\n🎉 All 8 officers resolved!")
    print(f"   Total officers in mapping: {len(mapping_rows)}")
    
    active_count = sum(1 for row in mapping_rows if row.get('Is_Active') == 'Y' and row['Salesforce_User_Id'] != 'ERROR')
    print(f"   Active officers: {active_count}")
else:
    print("\n⚠️  Some officers still have errors - manual intervention needed")

print("=" * 80)
