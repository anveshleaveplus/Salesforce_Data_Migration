"""
Create Field Officer Salesforce users programmatically
ONLY RUN THIS IF check_user_creation_permission.py shows you have admin rights
"""

import os
import csv
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Load SIT environment (same as sit_contact_load.py)
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)
print(f"Using environment: {env_file}\n")

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("=" * 80)
print("CREATING FIELD OFFICER SALESFORCE USERS")
print("=" * 80)

# Step 1: Get Profile ID (use Standard User or custom Field Officer profile)
print("\n[1] Getting Profile ID...")

# Try to find a suitable profile
profiles = sf.query("""
    SELECT Id, Name 
    FROM Profile 
    WHERE Name IN ('Standard User', 'Field Service User', 'Standard Platform User')
    ORDER BY Name
""")

if profiles['totalSize'] == 0:
    print("✗ No suitable profile found!")
    exit(1)

profile = profiles['records'][0]
print(f"✓ Using Profile: {profile['Name']} (ID: {profile['Id']})")

# Step 2: Load Field Officers from CSV
print("\n[2] Loading Field Officers from CSV...")

officers_to_create = []
with open('field_officers_from_oracle.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        officers_to_create.append(row)

print(f"✓ Found {len(officers_to_create)} officers to create")

# Step 3: Create users
print("\n[3] Creating Salesforce Users...")
print("-" * 80)

created = []
errors = []

for officer in officers_to_create:
    officer_code = officer['Officer_Code']
    user_name = officer['User_Name']
    mobile = officer.get('Mobile_Phone', '')
    is_active = officer.get('Is_Active', 'Y') == 'Y'
    
    # Parse first and last names
    name_parts = user_name.split(' ', 1)
    first_name = name_parts[0] if len(name_parts) > 0 else officer_code
    last_name = name_parts[1] if len(name_parts) > 1 else officer_code
    
    # Generate unique email for SIT (add .sit suffix to avoid prod conflicts)
    name_for_email = user_name.lower().replace(' - o', '').replace(' ', '.').strip()
    email = f"{name_for_email}.sit@leaveplus.com.au"
    
    # Generate alias (max 8 chars)
    alias = officer_code[:8] if len(officer_code) <= 8 else officer_code[:8]
    
    user_data = {
        'Username': email,
        'Email': email,
        'FirstName': first_name,
        'LastName': last_name,
        'Alias': alias,
        'CommunityNickname': officer_code,
        'ProfileId': profile['Id'],
        'TimeZoneSidKey': 'Australia/Sydney',
        'LocaleSidKey': 'en_AU',
        'EmailEncodingKey': 'UTF-8',
        'LanguageLocaleKey': 'en_US',
        'MobilePhone': mobile if mobile else None,
        'IsActive': is_active
    }
    
    try:
        # Check if user already exists
        existing = sf.query(f"SELECT Id FROM User WHERE Username = '{email}'")
        
        if existing['totalSize'] > 0:
            print(f"⚠️  {officer_code} ({email}): Already exists")
            created.append({
                'Officer_Code': officer_code,
                'Salesforce_User_Id': existing['records'][0]['Id'],
                'Email': email,
                'Status': 'Already Exists',
                'Is_Active': 'Y' if is_active else 'N'
            })
        else:
            # Create new user
            result = sf.User.create(user_data)
            
            if result['success']:
                print(f"✅ {officer_code} ({email}): Created successfully")
                created.append({
                    'Officer_Code': officer_code,
                    'Salesforce_User_Id': result['id'],
                    'Email': email,
                    'Status': 'Created',
                    'Is_Active': 'Y' if is_active else 'N'
                })
            else:
                print(f"❌ {officer_code} ({email}): {result['errors']}")
                errors.append({
                    'Officer_Code': officer_code,
                    'Email': email,
                    'Error': str(result['errors'])
                })
    
    except Exception as e:
        error_msg = str(e)
        # If duplicate, try with numbered suffix
        if 'DUPLICATE_USERNAME' in error_msg or 'DUPLICATE_VALUE' in error_msg:
            try:
                # Try with .2 suffix
                email_alt = email.replace('.sit@', '.sit2@')
                user_data['Username'] = email_alt
                user_data['Email'] = email_alt
                
                result = sf.User.create(user_data)
                if result['success']:
                    print(f"✅ {officer_code} ({email_alt}): Created with alternate email")
                    created.append({
                        'Officer_Code': officer_code,
                        'Salesforce_User_Id': result['id'],
                        'Email': email_alt,
                        'Status': 'Created (Alt)',
                        'Is_Active': 'Y' if is_active else 'N'
                    })
                else:
                    print(f"❌ {officer_code} ({email_alt}): {result['errors']}")
                    errors.append({
                        'Officer_Code': officer_code,
                        'Email': email_alt,
                        'Error': str(result['errors'])
                    })
            except Exception as e2:
                print(f"❌ {officer_code}: {str(e2)}")
                errors.append({
                    'Officer_Code': officer_code,
                    'Email': email,
                    'Error': str(e2)
                })
        else:
            print(f"❌ {officer_code} ({email}): {error_msg}")
            errors.append({
                'Officer_Code': officer_code,
                'Email': email,
                'Error': error_msg
            })

# Step 4: Export mapping
print("\n[4] Exporting User ID Mapping...")

if created:
    with open('field_officer_salesforce_mapping.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['Officer_Code', 'Salesforce_User_Id', 'Email', 'Status', 'Is_Active'])
        writer.writeheader()
        writer.writerows(created)
    
    print(f"✅ Exported: field_officer_salesforce_mapping.csv")

# Step 5: Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Created: {len([c for c in created if 'Created' in c['Status']])} users")
print(f"⚠️  Already Existed: {len([c for c in created if c['Status'] == 'Already Exists'])} users")
print(f"   - Active: {len([c for c in created if c.get('Is_Active') == 'Y'])} officers")
print(f"   - Inactive: {len([c for c in created if c.get('Is_Active') == 'N'])} officers")
print(f"❌ Errors: {len(errors)} users")

if errors:
    print("\nERRORS:")
    for err in errors:
        print(f"  {err['Officer_Code']}: {err['Error']}")

print("\n💡 Next Steps:")
print("  1. Review field_officer_salesforce_mapping.csv")
print("  2. Update sit_contact_load.py with user ID mappings")
print("  3. Re-run contact load with FieldOfficerAllocated__c")

print("=" * 80)
