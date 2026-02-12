"""
Create JJO and MOB with sit2 suffix (sit1 was used in PROTO environment)
"""
import os
import csv
from dotenv import load_dotenv
from simple_salesforce import Salesforce

env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("=" * 80)
print("CREATING JJO AND MOB (with sit2 suffix)")
print("=" * 80)

# Get profile
profile = sf.query("SELECT Id FROM Profile WHERE Name = 'Standard User' LIMIT 1")['records'][0]

officers = [
    {
        'code': 'JJO',
        'username': 'john.odea.sit2@leaveplus.com.au',
        'first': 'John',
        'last': 'Odea',
        'active': 'N'
    },
    {
        'code': 'MOB',
        'username': 'michael.obrien.sit2@leaveplus.com.au',
        'first': 'Michael',
        'last': 'Obrien',
        'active': 'N'
    }
]

results = []

for officer in officers:
    code = officer['code']
    username = officer['username']
    
    print(f"\n{code} ({officer['first']} {officer['last']})")
    print("-" * 80)
    
    try:
        user_data = {
            'Username': username,
            'Email': username,
            'FirstName': officer['first'],
            'LastName': officer['last'],
            'Alias': code.lower(),
            'ProfileId': profile['Id'],
            'TimeZoneSidKey': 'Australia/Sydney',
            'LocaleSidKey': 'en_AU',
            'EmailEncodingKey': 'UTF-8',
            'LanguageLocaleKey': 'en_US',
            'IsActive': True
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
        print(f"  ❌ Failed: {error_msg[:150]}")
        results.append({
            'code': code,
            'user_id': 'ERROR',
            'email': username,
            'status': f'Error: {error_msg[:50]}',
            'active': officer['active']
        })

# Update mapping
print("\n" + "=" * 80)
print("UPDATING MAPPING FILE")
print("=" * 80)

mapping_rows = []
with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    mapping_rows = list(reader)

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

with open('field_officer_salesforce_mapping.csv', 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['Officer_Code', 'Salesforce_User_Id', 'Email', 'Status', 'Is_Active']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(mapping_rows)

print(f"✅ Updated field_officer_salesforce_mapping.csv")

# Final summary
print("\n" + "=" * 80)
print("FINAL SUMMARY")
print("=" * 80)

total = len(mapping_rows)
working = sum(1 for row in mapping_rows if row['Salesforce_User_Id'] != 'ERROR')
active = sum(1 for row in mapping_rows if row.get('Is_Active') == 'Y' and row['Salesforce_User_Id'] != 'ERROR')

created = sum(1 for r in results if 'Created' in r.get('status', ''))
errors = sum(1 for r in results if 'ERROR' in r.get('user_id', ''))

print(f"This run:")
print(f"  ✅ Created: {created}")
print(f"  ❌ Errors: {errors}")

print(f"\nTotal Field Officers:")
print(f"  Total Officers: {total}")
print(f"  Working IDs: {working}/{total}")
print(f"  Active Officers: {active}")

if errors == 0:
    print(f"\n🎉 Field Officer user creation complete!")
else:
    print(f"\n⚠️  {errors} officer(s) still need manual creation")

print("=" * 80)
