"""
Fix the 4 remaining Field Officer users with issues
"""

import os
import csv
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv()

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("=" * 80)
print("FIXING REMAINING 4 FIELD OFFICERS")
print("=" * 80)

# Get profile
profiles = sf.query("SELECT Id FROM Profile WHERE Name = 'Standard User' LIMIT 1")
profile_id = profiles['records'][0]['Id']

# Fix 1: JEREMYT - already exists, just need to get ID
print("\n[1] JEREMYT - Finding existing user...")
try:
    existing = sf.query("SELECT Id FROM User WHERE Username = 'jeremy.tobin@leaveplus.com.au'")
    if existing['totalSize'] > 0:
        print(f"✅ Found existing: {existing['records'][0]['Id']}")
        jeremyt_id = existing['records'][0]['Id']
        jeremyt_email = 'jeremy.tobin@leaveplus.com.au'
    else:
        print("❌ Not found, will create with alt nickname")
        result = sf.User.create({
            'Username': 'jeremy.tobin.sit@leaveplus.com.au',
            'Email': 'jeremy.tobin.sit@leaveplus.com.au',
            'FirstName': 'Jeremy',
            'LastName': 'Tobin',
            'Alias': 'JEREMYT',
            'CommunityNickname': 'JEREMYT2',
            'ProfileId': profile_id,
            'TimeZoneSidKey': 'Australia/Sydney',
            'LocaleSidKey': 'en_AU',
            'EmailEncodingKey': 'UTF-8',
            'LanguageLocaleKey': 'en_US',
            'IsActive': True
        })
        print(f"✅ Created: {result['id']}")
        jeremyt_id = result['id']
        jeremyt_email = 'jeremy.tobin.sit@leaveplus.com.au'
except Exception as e:
    print(f"❌ Error: {e}")
    jeremyt_id = None
    jeremyt_email = None

# Fix 2: WJS - already exists
print("\n[2] WJS - Finding existing user...")
try:
    existing = sf.query("SELECT Id FROM User WHERE Username = 'jude.fernando@leaveplus.com.au'")
    if existing['totalSize'] > 0:
        print(f"✅ Found existing: {existing['records'][0]['Id']}")
        wjs_id = existing['records'][0]['Id']
        wjs_email = 'jude.fernando@leaveplus.com.au'
    else:
        result = sf.User.create({
            'Username': 'jude.fernando.sit@leaveplus.com.au',
            'Email': 'jude.fernando.sit@leaveplus.com.au',
            'FirstName': 'Jude',
            'LastName': 'Fernando',
            'Alias': 'WJS',
            'CommunityNickname': 'WJS2',
            'ProfileId': profile_id,
            'TimeZoneSidKey': 'Australia/Sydney',
            'LocaleSidKey': 'en_AU',
            'EmailEncodingKey': 'UTF-8',
            'LanguageLocaleKey': 'en_US',
            'IsActive': True
        })
        print(f"✅ Created: {result['id']}")
        wjs_id = result['id']
        wjs_email = 'jude.fernando.sit@leaveplus.com.au'
except Exception as e:
    print(f"❌ Error: {e}")
    wjs_id = None
    wjs_email = None

# Fix 3: JJO - John O'Dea (apostrophe issue)
print("\n[3] JJO - Creating John O'Dea...")
try:
    result = sf.User.create({
        'Username': 'john.odea.sit@leaveplus.com.au',
        'Email': 'john.odea.sit@leaveplus.com.au',
        'FirstName': 'John',
        'LastName': "O'Dea",
        'Alias': 'JJO',
        'CommunityNickname': 'JJO',
        'ProfileId': profile_id,
        'TimeZoneSidKey': 'Australia/Sydney',
        'LocaleSidKey': 'en_AU',
        'EmailEncodingKey': 'UTF-8',
        'LanguageLocaleKey': 'en_US',
        'IsActive': False
    })
    print(f"✅ Created: {result['id']}")
    jjo_id = result['id']
    jjo_email = 'john.odea.sit@leaveplus.com.au'
except Exception as e:
    print(f"❌ Error: {e}")
    jjo_id = None
    jjo_email = None

# Fix 4: MOB - Michael O'Brien (apostrophe issue)
print("\n[4] MOB - Creating Michael O'Brien...")
try:
    result = sf.User.create({
        'Username': 'michael.obrien.sit@leaveplus.com.au',
        'Email': 'michael.obrien.sit@leaveplus.com.au',
        'FirstName': 'Michael',
        'LastName': "O'Brien",
        'Alias': 'MOB',
        'CommunityNickname': 'MOB',
        'ProfileId': profile_id,
        'TimeZoneSidKey': 'Australia/Sydney',
        'LocaleSidKey': 'en_AU',
        'EmailEncodingKey': 'UTF-8',
        'LanguageLocaleKey': 'en_US',
        'IsActive': False
    })
    print(f"✅ Created: {result['id']}")
    mob_id = result['id']
    mob_email = 'michael.obrien.sit@leaveplus.com.au'
except Exception as e:
    print(f"❌ Error: {e}")
    mob_id = None
    mob_email = None

# Update CSV
print("\n[5] Updating mapping CSV...")
new_rows = []

if jeremyt_id:
    new_rows.append({'Officer_Code': 'JEREMYT', 'Salesforce_User_Id': jeremyt_id, 'Email': jeremyt_email, 'Status': 'Already Exists', 'Is_Active': 'Y'})
if wjs_id:
    new_rows.append({'Officer_Code': 'WJS', 'Salesforce_User_Id': wjs_id, 'Email': wjs_email, 'Status': 'Already Exists', 'Is_Active': 'Y'})
if jjo_id:
    new_rows.append({'Officer_Code': 'JJO', 'Salesforce_User_Id': jjo_id, 'Email': jjo_email, 'Status': 'Created', 'Is_Active': 'N'})
if mob_id:
    new_rows.append({'Officer_Code': 'MOB', 'Salesforce_User_Id': mob_id, 'Email': mob_email, 'Status': 'Created', 'Is_Active': 'N'})

# Read existing
existing_data = []
with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    existing_data = list(reader)

# Remove old entries if they exist
existing_codes = [row['Officer_Code'] for row in existing_data]
for new_row in new_rows:
    if new_row['Officer_Code'] in existing_codes:
        existing_data = [r for r in existing_data if r['Officer_Code'] != new_row['Officer_Code']]

# Add new rows
existing_data.extend(new_rows)

# Sort by officer code
existing_data.sort(key=lambda x: x['Officer_Code'])

# Write back
with open('field_officer_salesforce_mapping.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Officer_Code', 'Salesforce_User_Id', 'Email', 'Status', 'Is_Active'])
    writer.writeheader()
    writer.writerows(existing_data)

print(f"✅ Updated mapping CSV with {len(new_rows)} officers")

print("\n" + "=" * 80)
print(f"FINAL TOTAL: {len(existing_data)} Field Officers mapped")
print("=" * 80)
