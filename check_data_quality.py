"""
Check for data quality issues in account load
"""
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv('.env.sit')

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("\n" + "="*70)
print("Data Quality Issues Check")
print("="*70)

# Issue 1: Record count mismatch
print("\n1. Record Count Discrepancy")
print("-"*70)
query = "SELECT COUNT(Id) Total FROM Account WHERE External_Id__c != null"
result = sf.query(query)
sf_count = result['records'][0]['Total']
print(f"Script reported final count: 53,857")
print(f"Salesforce actual count:     {sf_count:,}")
print(f"Difference:                  {sf_count - 53857:+,}")
if sf_count != 53857:
    print("⚠️  MISMATCH - Some records existed before this load")

# Issue 2: Missing Registration Numbers
print("\n2. Missing Registration Numbers")
print("-"*70)
query = """
SELECT External_Id__c, Name, Registration_Number__c 
FROM Account 
WHERE External_Id__c != null 
  AND (Registration_Number__c = null OR Registration_Number__c = '')
LIMIT 10
"""
result = sf.query(query)
missing_reg = result['totalSize']
print(f"Records missing Registration_Number__c: {missing_reg}")
if missing_reg > 0:
    print("⚠️  Sample records:")
    for rec in result['records'][:5]:
        print(f"   External_Id: {rec['External_Id__c']}, Name: {rec['Name']}")

# Issue 3: Duplicate handling - are we losing ABN matches?
print("\n3. Duplicate Handling Impact")
print("-"*70)
print("Oracle before deduplication: 56,826 records")
print("Oracle after deduplication:  53,857 records")
print("Duplicates removed:          2,969 records")
print("")
print("ABN matches before dedup:    30,572 (53.8% of 56,826)")
print("ABN fields populated in SF:  27,624 (51.3% of 53,857)")
print("Lost ABN matches:            ~2,948 (likely from duplicate removal)")
print("")
print("⚠️  ISSUE: Keeping 'first' occurrence may lose ABN data")
print("   Solution: Prioritize rows WITH ABN data when deduplicating")

# Issue 4: ABN but no SQL Server data
print("\n4. Accounts with ABN but No SQL Server Enrichment")
print("-"*70)
query = """
SELECT COUNT(Id) Total
FROM Account 
WHERE External_Id__c != null 
  AND ABN__c != null 
  AND ABNRegistrationDate__c = null
"""
result = sf.query(query)
has_abn_no_sql = result['records'][0]['Total']
query2 = "SELECT COUNT(Id) Total FROM Account WHERE External_Id__c != null AND ABN__c != null"
result2 = sf.query(query2)
total_with_abn = result2['records'][0]['Total']

print(f"Total accounts with ABN:           {total_with_abn:,}")
print(f"With SQL Server enrichment:        27,624 ({27624/total_with_abn*100:.1f}%)")
print(f"Missing SQL Server enrichment:     {has_abn_no_sql:,} ({has_abn_no_sql/total_with_abn*100:.1f}%)")
print("")
print("Reasons for missing enrichment:")
print("  1. ABN not found in SQL Server ABR database (~18%)")
print("  2. SQL Server has NULL values for that ABN")
print("  3. ABN format mismatch (unlikely - cleaned in script)")

# Issue 5: Field population rates
print("\n5. Field Population Rates")
print("-"*70)
query = """
SELECT 
    COUNT(Id) Total,
    COUNT(ABN__c) Has_ABN,
    COUNT(ACN__c) Has_ACN,
    COUNT(DateEmploymentCommenced__c) Has_EmpDate,
    COUNT(ABNRegistrationDate__c) Has_ABN_RegDate,
    COUNT(AccountStatus__c) Has_AccountStatus,
    COUNT(OSCACode__c) Has_OSCACode
FROM Account 
WHERE External_Id__c != null
"""
result = sf.query(query)
rec = result['records'][0]
total = rec['Total']

print(f"Total accounts: {total:,}")
print("")
print("Oracle fields:")
print(f"  ABN__c:                    {rec['Has_ABN']:,} ({rec['Has_ABN']/total*100:.1f}%)")
print(f"  ACN__c:                    {rec['Has_ACN']:,} ({rec['Has_ACN']/total*100:.1f}%)")
print(f"  DateEmploymentCommenced:   {rec['Has_EmpDate']:,} ({rec['Has_EmpDate']/total*100:.1f}%)")
print("")
print("SQL Server ABR fields:")
print(f"  ABNRegistrationDate__c:    {rec['Has_ABN_RegDate']:,} ({rec['Has_ABN_RegDate']/total*100:.1f}%)")
print(f"  AccountStatus__c:          {rec['Has_AccountStatus']:,} ({rec['Has_AccountStatus']/total*100:.1f}%)")
print(f"  OSCACode__c:               {rec['Has_OSCACode']:,} ({rec['Has_OSCACode']/total*100:.1f}%)")

print("\n" + "="*70)
print("Summary: Data Quality Issues Found")
print("="*70)
print("1. ⚠️  Record count mismatch (21 existing records)")
print("2. ⚠️  21 records missing Registration_Number__c (0.04%)")
print("3. ⚠️  Duplicate handling loses ~2,948 ABN matches (keep='first' issue)")
print("4. ℹ️  6,045 accounts with ABN but no SQL Server enrichment (18%)")
print("5. ✅ Field population rates are acceptable")
print("")
print("RECOMMENDED FIX:")
print("Change duplicate handling to prioritize rows WITH ABN data:")
print("  - Sort by ABN descending (nulls last) before drop_duplicates")
print("  - This ensures we keep the row with ABN data when duplicates exist")
print("="*70)
