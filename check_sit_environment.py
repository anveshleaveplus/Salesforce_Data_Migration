"""
Check SIT Environment - Get overview of data loaded
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
print("SALESFORCE SIT ENVIRONMENT OVERVIEW")
print("=" * 80)
print(f"Environment: {os.getenv('SF_USERNAME')}")
print(f"Domain: {sf.sf_instance}")
print()

# 1. Check Accounts
print("-" * 80)
print("ACCOUNTS")
print("-" * 80)
account_count = sf.query("SELECT COUNT() FROM Account")['totalSize']
print(f"Total Accounts: {account_count:,}")

# Sample account
account_sample = sf.query("SELECT Id, Name, External_Id__c, BillingCity FROM Account LIMIT 1")
if account_sample['records']:
    acc = account_sample['records'][0]
    print(f"Sample: {acc['Name']} (External_Id: {acc.get('External_Id__c')})")

# 2. Check Contacts
print("\n" + "-" * 80)
print("CONTACTS")
print("-" * 80)
contact_count = sf.query("SELECT COUNT() FROM Contact")['totalSize']
print(f"Total Contacts: {contact_count:,}")

# Sample contact
contact_sample = sf.query("SELECT Id, Name, External_Id__c, Email, AccountId FROM Contact LIMIT 1")
if contact_sample['records']:
    con = contact_sample['records'][0]
    print(f"Sample: {con['Name']} (External_Id: {con.get('External_Id__c')})")

# Check FieldOfficerAllocated__c
field_officer_count = sf.query("SELECT COUNT() FROM Contact WHERE FieldOfficerAllocated__c != null")['totalSize']
print(f"Contacts with Field Officer: {field_officer_count}")

# 3. Check AccountContactRelation
print("\n" + "-" * 80)
print("ACCOUNT CONTACT RELATIONS (Multiple Employers)")
print("-" * 80)
acr_count = sf.query("SELECT COUNT() FROM AccountContactRelation WHERE IsDirect = false")['totalSize']
print(f"Total ACRs (indirect): {acr_count:,}")

# 4. Check Users (Field Officers)
print("\n" + "-" * 80)
print("USERS (Field Officers)")
print("-" * 80)
fo_users = sf.query("SELECT COUNT() FROM User WHERE Email LIKE '%.sit2@leaveplus.com.au'")['totalSize']
print(f"Field Officer Users (.sit2): {fo_users}")

# Check active vs inactive
active_users = sf.query("SELECT COUNT() FROM User WHERE Email LIKE '%.sit2@leaveplus.com.au' AND IsActive = true")['totalSize']
print(f"  Active: {active_users}")
print(f"  Inactive: {fo_users - active_users}")

# 5. Check custom objects (if any)
print("\n" + "-" * 80)
print("CUSTOM OBJECTS")
print("-" * 80)

# Try to query some potential custom objects
custom_objects = []
potential_objects = [
    'Employment__c',
    'ServicePeriod__c', 
    'Leave__c',
    'LeaveRequest__c',
    'Return__c'
]

for obj in potential_objects:
    try:
        count = sf.query(f"SELECT COUNT() FROM {obj}")['totalSize']
        custom_objects.append((obj, count))
        print(f"{obj}: {count:,} records")
    except:
        pass

if not custom_objects:
    print("No custom objects found (or not yet loaded)")

# 6. Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Accounts: {account_count:,}")
print(f"✅ Contacts: {contact_count:,}")
print(f"✅ ACRs: {acr_count:,}")
print(f"✅ Field Officer Users: {fo_users} ({active_users} active)")
print(f"✅ Contacts with Field Officers: {field_officer_count}")

if custom_objects:
    print(f"\nCustom Objects:")
    for obj, count in custom_objects:
        print(f"  - {obj}: {count:,}")

print("\n" + "=" * 80)
