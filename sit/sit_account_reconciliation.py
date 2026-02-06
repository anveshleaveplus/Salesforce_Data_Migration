"""
Quick reconciliation report for SIT accounts
Runs data quality checks without reloading data
"""

import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from datetime import datetime

# Load .env.sit
load_dotenv('.env.sit')

# Connect to Salesforce
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("\nUsing environment: .env.sit")
print("=" * 70)
print("SIT ACCOUNT RECONCILIATION REPORT")
print("=" * 70)

# 1. Count records
print("\n[1/5] Record Counts")
print("-" * 70)
oracle_count = 53857  # From last load
sf_count = sf.query(f"SELECT COUNT() FROM Account WHERE External_Id__c != null")['totalSize']
print(f"  Oracle extracted:  {oracle_count:,} records")
print(f"  Salesforce total:  {sf_count:,} records with External_Id__c")
print(f"  Match status:      {'✓ MATCH' if oracle_count == sf_count else '✗ MISMATCH'}")

# 2. Data Quality - Missing required fields
print("\n[2/5] Data Quality - Required Fields")
print("-" * 70)
dq_query = """
SELECT COUNT() 
FROM Account 
WHERE External_Id__c != null 
AND (Name = null OR ABN__c = null)
"""
missing = sf.query(dq_query)['totalSize']
print(f"  Records with missing Name/ABN: {missing:,}")
print(f"  Status: {'✗ ISSUES FOUND' if missing > 0 else '✓ PASS'}")

# 3. Duplicate check
print("\n[3/5] Duplicate Check")
print("-" * 70)
dup_query = """
SELECT External_Id__c, COUNT(Id) cnt
FROM Account
WHERE External_Id__c != null
GROUP BY External_Id__c
HAVING COUNT(Id) > 1
"""
duplicates = sf.query_all(dup_query)
dup_count = duplicates['totalSize']
print(f"  Duplicate External_Id__c values: {dup_count:,}")
print(f"  Status: {'✗ DUPLICATES FOUND' if dup_count > 0 else '✓ PASS'}")

# 4. Field population rates
print("\n[4/5] Field Population Rates")
print("-" * 70)
fields = [
    'ABN__c',
    'ACN__c', 
    'RegisteredEntityName__c',
    'TradingAs__c',
    'Registration_Number__c',
    'DateEmploymentCommenced__c',
    'Type'
]

for field in fields:
    try:
        query = f"SELECT COUNT() FROM Account WHERE External_Id__c != null AND {field} != null"
        count = sf.query(query)['totalSize']
        pct = (count / sf_count * 100) if sf_count > 0 else 0
        print(f"  {field:30} {count:6,} ({pct:5.1f}%)")
    except Exception as e:
        print(f"  {field:30} [Field not found]")

# 5. Sample records
print("\n[5/5] Sample Records")
print("-" * 70)
sample_query = """
SELECT Id, Name, External_Id__c, ABN__c, Type, CreatedDate
FROM Account
WHERE External_Id__c != null
ORDER BY CreatedDate DESC
LIMIT 5
"""
samples = sf.query(sample_query)
for i, rec in enumerate(samples['records'], 1):
    print(f"  {i}. {rec['Name'][:40]:40} | ABN: {rec.get('ABN__c', 'N/A'):15} | Type: {rec.get('Type', 'N/A')}")

print("\n" + "=" * 70)
print("RECONCILIATION COMPLETE")
print("=" * 70)

# Save report
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
report_file = f'test_output/sit_account_reconciliation_{timestamp}.txt'
os.makedirs('test_output', exist_ok=True)

with open(report_file, 'w') as f:
    f.write(f"SIT Account Reconciliation Report\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"="*70 + "\n\n")
    f.write(f"Oracle Count: {oracle_count:,}\n")
    f.write(f"Salesforce Count: {sf_count:,}\n")
    f.write(f"Match: {'Yes' if oracle_count == sf_count else 'No'}\n")
    f.write(f"Missing Data: {missing:,}\n")
    f.write(f"Duplicates: {dup_count:,}\n")

print(f"\n📝 Report saved to: {report_file}")
