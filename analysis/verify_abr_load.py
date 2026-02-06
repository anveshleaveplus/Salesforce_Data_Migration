"""
Verify SQL Server ABR enrichment fields loaded successfully
"""
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Load SIT credentials
load_dotenv('.env.sit')

# Connect to Salesforce
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("\n" + "="*70)
print("SQL Server ABR Field Verification")
print("="*70)

# Query to check ABR field population
query = """
SELECT 
    COUNT(Id) Total_Accounts,
    COUNT(ABN__c) Has_ABN,
    COUNT(ABNRegistrationDate__c) Has_ABN_RegDate,
    COUNT(AccountStatus__c) Has_AccountStatus,
    COUNT(OSCACode__c) Has_OSCACode
FROM Account 
WHERE External_Id__c != null
"""

result = sf.query(query)
record = result['records'][0]

print(f"\nTotal Accounts:              {record['Total_Accounts']:,}")
print(f"Accounts with ABN:           {record['Has_ABN']:,}")
print(f"")
print(f"SQL Server ABR Fields:")
print(f"  ABNRegistrationDate__c:    {record['Has_ABN_RegDate']:,} ({record['Has_ABN_RegDate']/record['Total_Accounts']*100:.1f}%)")
print(f"  AccountStatus__c:          {record['Has_AccountStatus']:,} ({record['Has_AccountStatus']/record['Total_Accounts']*100:.1f}%)")
print(f"  OSCACode__c:               {record['Has_OSCACode']:,} ({record['Has_OSCACode']/record['Total_Accounts']*100:.1f}%)")

# Get sample records with ABR data
print(f"\n" + "="*70)
print("Sample Records with SQL Server ABR Data")
print("="*70)

sample_query = """
SELECT 
    External_Id__c,
    Name,
    ABN__c,
    ABNRegistrationDate__c,
    AccountStatus__c,
    OSCACode__c
FROM Account 
WHERE External_Id__c != null 
  AND ABNRegistrationDate__c != null
ORDER BY External_Id__c
LIMIT 5
"""

result = sf.query(sample_query)
for rec in result['records']:
    print(f"\nExternal ID: {rec['External_Id__c']}")
    print(f"  Name: {rec['Name']}")
    print(f"  ABN: {rec['ABN__c']}")
    print(f"  ABN Reg Date: {rec['ABNRegistrationDate__c']}")
    print(f"  Account Status: {rec['AccountStatus__c']}")
    print(f"  OSCA Code: {rec['OSCACode__c']}")

# Check AccountStatus__c picklist values used
print(f"\n" + "="*70)
print("AccountStatus__c Value Distribution")
print("="*70)

status_query = """
SELECT 
    AccountStatus__c,
    COUNT(Id) Count
FROM Account 
WHERE External_Id__c != null 
  AND AccountStatus__c != null
GROUP BY AccountStatus__c
ORDER BY COUNT(Id) DESC
"""

result = sf.query(status_query)
for rec in result['records']:
    print(f"  {rec['AccountStatus__c']}: {rec['Count']:,}")

print("\n" + "="*70)
print("✓ SQL Server ABR Enrichment Verification Complete")
print("="*70)
