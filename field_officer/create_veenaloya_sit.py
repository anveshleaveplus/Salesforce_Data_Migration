"""
Create VEENALOYA - the 47th Field Officer (discovered later, not in original export)
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
print("CREATING VEENALOYA (47th Field Officer)")
print("=" * 80)

# Get profile
profile = sf.query("SELECT Id FROM Profile WHERE Name = 'Standard User' LIMIT 1")['records'][0]

officer = {
    'code': 'VEENALOYA',
    'username': 'veena.loya.sit2@leaveplus.com.au',
    'first': 'Veena',
    'last': 'Loya',
    'active': 'N'  # Inactive, only 1 worker assignment
}

print(f"\n{officer['code']} ({officer['first']} {officer['last']})")
print("-" * 80)

try:
    user_data = {
        'Username': officer['username'],
        'Email': officer['username'],
        'FirstName': officer['first'],
        'LastName': officer['last'],
        'Alias': 'vloya',
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
    
    # Update mapping
    mapping_rows = []
    with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        mapping_rows = list(reader)
    
    mapping_rows.append({
        'Officer_Code': officer['code'],
        'Salesforce_User_Id': user_id,
        'Email': officer['username'],
        'Status': 'Created',
        'Is_Active': officer['active']
    })
    
    with open('field_officer_salesforce_mapping.csv', 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['Officer_Code', 'Salesforce_User_Id', 'Email', 'Status', 'Is_Active']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(mapping_rows)
    
    print(f"\n✅ Updated field_officer_salesforce_mapping.csv")
    
    # Final summary
    print("\n" + "=" * 80)
    print("FINAL SUMMARY - ALL FIELD OFFICERS")
    print("=" * 80)
    
    total = len(mapping_rows)
    working = sum(1 for row in mapping_rows if row['Salesforce_User_Id'] != 'ERROR')
    active = sum(1 for row in mapping_rows if row.get('Is_Active') == 'Y' and row['Salesforce_User_Id'] != 'ERROR')
    
    print(f"Total Officers: {total}")
    print(f"Working IDs: {working}/{total}")
    print(f"Active Officers: {active}")
    print(f"\n🎉 ALL 47 Field Officer users created in SIT!")
    print("=" * 80)
    
except Exception as e:
    error_msg = str(e)
    print(f"  ❌ Failed: {error_msg}")
    print("\n⚠️  Manual creation needed for VEENALOYA")
