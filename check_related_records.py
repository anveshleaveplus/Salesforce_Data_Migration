from dotenv import load_dotenv
import os
from simple_salesforce import Salesforce

load_dotenv('.env.sit')
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("Checking for related records blocking deletion:")
print("="*70)

# Check Contacts
result = sf.query("SELECT COUNT() FROM Contact WHERE AccountId IN (SELECT Id FROM Account WHERE External_Id__c != null)")
contact_count = result['totalSize']
print(f"Contacts linked to test accounts: {contact_count:,}")

# Check ACRs
result = sf.query("SELECT COUNT() FROM AccountContactRelation WHERE AccountId IN (SELECT Id FROM Account WHERE External_Id__c != null)")
acr_count = result['totalSize']
print(f"AccountContactRelations: {acr_count:,}")

# Check Opportunities
try:
    result = sf.query("SELECT COUNT() FROM Opportunity WHERE AccountId IN (SELECT Id FROM Account WHERE External_Id__c != null)")
    opp_count = result['totalSize']
    print(f"Opportunities: {opp_count:,}")
except:
    print(f"Opportunities: N/A")

print(f"\n{'='*70}")
if contact_count > 0 or acr_count > 0:
    print("⚠️  Related records exist - deletion will be SLOW or FAIL")
    print("    Salesforce must delete child records first")
    print("\n💡 RECOMMENDATION: Skip deletion, just run upsert load")
    print("    - Existing 10K will update")
    print("    - New ~44K will insert")
    print("    - Much faster and safer!")
else:
    print("✅ No related records - safe to delete")
