"""
Fix JJO and MOB - officers with apostrophes in last names
Try different approaches to handle apostrophes
"""
import os
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
print("FIXING APOSTROPHE OFFICERS (JJO, MOB)")
print("=" * 80)

# Get Standard User profile
profile = sf.query("SELECT Id FROM Profile WHERE Name = 'Standard User' LIMIT 1")['records'][0]

# Try multiple approaches for each officer
officers = [
    {
        'code': 'JJO',
        'username': 'john.odea.sit@leaveplus.com.au',
        'email': 'john.odea.sit@leaveplus.com.au',
        'first': 'John',
        'last_options': ["O'Dea", "ODea", "Odea"],  # Try with and without apostrophe
        'active': 'N'
    },
    {
        'code': 'MOB',
        'username': 'michael.obrien.sit@leaveplus.com.au',
        'email': 'michael.obrien.sit@leaveplus.com.au',
        'first': 'Michael',
        'last_options': ["O'Brien", "OBrien", "Obrien"],  # Try with and without apostrophe
        'active': 'N'
    }
]

results = []

for officer in officers:
    code = officer['code']
    username = officer['username']
    
    print(f"\n{code} ({officer['first']} {officer['last_options'][0]})")
    print("-" * 80)
    
    # First check if already exists
    try:
        check_query = f"SELECT Id FROM User WHERE Username = '{username}'"
        existing = sf.query(check_query)
        
        if existing['totalSize'] > 0:
            user_id = existing['records'][0]['Id']
            print(f"  ✅ Already exists: {user_id}")
            results.append({
                'code': code,
                'user_id': user_id,
                'email': username,
                'status': 'Already Existed',
                'active': officer['active']
            })
            continue
    except Exception as e:
        print(f"  ⚠️  Check query failed: {str(e)[:80]}")
    
    # Try creating with different LastName variants
    created = False
    for last_name in officer['last_options']:
        if created:
            break
            
        print(f"  Trying LastName: {last_name}")
        
        try:
            user_data = {
                'Username': username,
                'Email': username,
                'FirstName': officer['first'],
                'LastName': last_name,
                'Alias': code.lower()[:8],
                'ProfileId': profile['Id'],
                'TimeZoneSidKey': 'Australia/Sydney',
                'LocaleSidKey': 'en_AU',
                'EmailEncodingKey': 'UTF-8',
                'LanguageLocaleKey': 'en_US',
                'IsActive': True
            }
            
            result = sf.User.create(user_data)
            user_id = result['id']
            print(f"  ✅ Created with LastName='{last_name}': {user_id}")
            
            results.append({
                'code': code,
                'user_id': user_id,
                'email': username,
                'status': f'Created (LastName: {last_name})',
                'active': officer['active']
            })
            created = True
            
        except Exception as e:
            error_msg = str(e)
            print(f"    ❌ Failed with '{last_name}': {error_msg[:80]}")
            
            # If this was the last option, record the error
            if last_name == officer['last_options'][-1] and not created:
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

import csv

# Read existing mapping
mapping_rows = []
with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    mapping_rows = list(reader)

# Update with new results
for result in results:
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

print(f"✅ Updated field_officer_salesforce_mapping.csv")

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

created = sum(1 for r in results if 'Created' in r['status'])
errors = sum(1 for r in results if 'ERROR' in r['user_id'])

print(f"✅ Created: {created}")
print(f"❌ Errors: {errors}")

if errors == 0:
    print("\n🎉 All apostrophe officers resolved!")
    
    # Count total in mapping
    total = len(mapping_rows)
    active = sum(1 for row in mapping_rows if row.get('Is_Active') == 'Y' and row['Salesforce_User_Id'] != 'ERROR')
    working = sum(1 for row in mapping_rows if row['Salesforce_User_Id'] != 'ERROR')
    
    print(f"   Total officers: {total}")
    print(f"   Working IDs: {working}/{total}")
    print(f"   Active officers: {active}")

print("=" * 80)
