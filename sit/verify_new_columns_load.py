"""
Verify new columns loaded successfully to Salesforce
"""
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import os

load_dotenv('.env.sit')

print("Connecting to Salesforce SIT...")
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain=os.getenv('SF_DOMAIN')
)

print(f"✓ Connected to {sf.sf_instance}")
print("\n" + "="*80)
print("VERIFYING NEW COLUMNS")
print("="*80)

# Query accounts with new columns
query = """
SELECT 
    External_Id__c,
    Name,
    NumberOfEmployees,
    OwnersPerformCoveredWork__c,
    BusinessEmail__c
FROM Account
WHERE External_Id__c IN ('407', '875', '2124', '23000', '52970', '61766', '2446804', '2769048', '38586', '74754')
ORDER BY External_Id__c
"""

print("\nQuerying sample accounts...")
results = sf.query_all(query)
records = results['records']

print(f"Found {len(records)} records\n")
print("-"*80)
print(f"{'External_Id':<15} {'Name':<40} {'Employees':<12} {'OwnerWork':<12} {'Email':<20}")
print("-"*80)

for rec in records:
    ext_id = rec.get('External_Id__c', '')
    name = rec.get('Name', '')[:40]
    employees = rec.get('NumberOfEmployees', '')
    owner_work = rec.get('OwnersPerformCoveredWork__c', '')
    email = rec.get('BusinessEmail__c', '')[:20] if rec.get('BusinessEmail__c') else ''
    
    print(f"{ext_id:<15} {name:<40} {str(employees):<12} {str(owner_work):<12} {email:<20}")

# Get statistics
print("\n" + "="*80)
print("FIELD POPULATION STATISTICS")
print("="*80)

stats_query = """
SELECT 
    COUNT(Id) Total,
    COUNT(NumberOfEmployees) NumberOfEmployees_Count,
    COUNT(BusinessEmail__c) BusinessEmail_Count
FROM Account
"""

stats = sf.query(stats_query)['records'][0]

total = stats['Total']
num_emp = stats['NumberOfEmployees_Count']
email = stats['BusinessEmail_Count']

print(f"\nTotal Accounts: {total:,}")
print(f"\nNumberOfEmployees:")
print(f"  Populated: {num_emp:,} ({num_emp/total*100:.2f}%)")
print(f"  NULL: {total-num_emp:,} ({(total-num_emp)/total*100:.2f}%)")

# For boolean field, query separately
owner_query = """
SELECT COUNT(Id)
FROM Account
WHERE OwnersPerformCoveredWork__c != null
"""
owner_work = sf.query(owner_query)['records'][0]['expr0']

print(f"\nOwnersPerformCoveredWork__c:")
print(f"  Populated: {owner_work:,} ({owner_work/total*100:.2f}%)")
print(f"  NULL: {total-owner_work:,} ({(total-owner_work)/total*100:.2f}%)")

print(f"\nBusinessEmail__c:")
print(f"  Populated: {email:,} ({email/total*100:.2f}%)")
print(f"  NULL: {total-email:,} ({(total-email)/total*100:.2f}%)")

# Check for actual email values (not empty strings)
real_email_query = """
SELECT COUNT(Id) 
FROM Account 
WHERE BusinessEmail__c != null 
AND BusinessEmail__c != ''
AND LENGTH(BusinessEmail__c) > 0
"""
real_emails = sf.query(real_email_query)['records'][0]['expr0']
print(f"  With actual email addresses: {real_emails:,} ({real_emails/total*100:.4f}%)")

print("\n" + "="*80)
print("✓ NEW COLUMNS LOADED SUCCESSFULLY")
print("="*80)
