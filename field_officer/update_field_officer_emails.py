"""
Update Field Officer Salesforce users with real email addresses
Run this AFTER receiving actual email addresses from Cherie

Instructions:
1. Get email list from Cherie
2. Create/update field_officer_real_emails.csv with columns:
   - Officer_Code
   - Real_Email
3. Run this script to update all Salesforce users in SIT
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
print("UPDATE FIELD OFFICER EMAIL ADDRESSES")
print("=" * 80)

# Load current mapping (has Salesforce User IDs)
current_mapping = {}
with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        current_mapping[row['Officer_Code']] = row

# Load real email addresses
real_emails_file = 'field_officer_real_emails.csv'

try:
    with open(real_emails_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        real_emails = {row['Officer_Code']: row['Real_Email'] for row in reader}
except FileNotFoundError:
    print(f"❌ File not found: {real_emails_file}")
    print("\nPlease create this file with columns:")
    print("  Officer_Code,Real_Email")
    print("\nExample:")
    print("  MICHAELD,michael.docherty@leaveplus.com.au")
    print("  JEREMYT,jeremy.tobin@leaveplus.com.au")
    exit(1)

print(f"\nLoaded {len(real_emails)} real email addresses")
print(f"Found {len(current_mapping)} officers in current mapping\n")

# Update Salesforce users
success = 0
errors = 0
skipped = 0

for officer_code, real_email in real_emails.items():
    if officer_code not in current_mapping:
        print(f"⚠️  {officer_code}: Not in mapping (skipped)")
        skipped += 1
        continue
    
    user_id = current_mapping[officer_code]['Salesforce_User_Id']
    current_email = current_mapping[officer_code]['Email']
    
    print(f"{officer_code} ({user_id}):")
    print(f"  Current: {current_email}")
    print(f"  New:     {real_email}")
    
    try:
        # Update Salesforce User
        sf.User.update(user_id, {
            'Email': real_email,
            'Username': real_email  # Username must also be updated for login
        })
        print(f"  ✅ Updated")
        success += 1
        
        # Update mapping file
        current_mapping[officer_code]['Email'] = real_email
        
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Failed: {error_msg[:80]}")
        errors += 1

# Write updated mapping
print("\n" + "=" * 80)
print("UPDATING MAPPING FILE")
print("=" * 80)

with open('field_officer_salesforce_mapping.csv', 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['Officer_Code', 'Salesforce_User_Id', 'Email', 'Status', 'Is_Active']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(current_mapping.values())

print(f"✅ Updated field_officer_salesforce_mapping.csv")

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Updated: {success}")
print(f"❌ Errors: {errors}")
print(f"⚠️  Skipped: {skipped}")

if errors == 0 and success > 0:
    print(f"\n🎉 Successfully updated {success} Field Officer email addresses!")
    print("   Users can now login with their real email addresses")
elif errors > 0:
    print(f"\n⚠️  {errors} updates failed - review errors above")

print("=" * 80)
