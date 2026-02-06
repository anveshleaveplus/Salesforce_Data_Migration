"""
Delete SIT Accounts by Creator Name
Filters accounts where CreatedBy.Name contains 'Anvesh'
"""

import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from datetime import datetime

# Load environment
load_dotenv('.env.sit')

# Connect
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("=" * 80)
print("SIT Account Deletion - By Creator Name")
print("=" * 80)

# Query accounts where CreatedBy.Name contains 'Anvesh'
query = """
SELECT Id, Name, External_Id__c, CreatedBy.Name, CreatedDate
FROM Account
WHERE CreatedBy.Name LIKE '%Anvesh%'
ORDER BY CreatedDate DESC
"""

print("\nQuerying accounts where CreatedBy.Name contains 'Anvesh'...")
result = sf.query_all(query)
accounts = result['records']

print(f"\n✅ Found {len(accounts):,} accounts")

if accounts:
    print("\nFirst 10 samples:")
    for i, acc in enumerate(accounts[:10], 1):
        print(f"{i}. {acc['Name']} | ExtId: {acc['External_Id__c']} | Creator: {acc['CreatedBy']['Name']} | Created: {acc['CreatedDate'][:10]}")
    
    if len(accounts) > 10:
        print(f"... and {len(accounts) - 10:,} more")
    
    print("\n" + "=" * 80)
    confirm = input(f"\n⚠️  DELETE {len(accounts):,} accounts? Type 'DELETE' to confirm: ")
    
    if confirm == 'DELETE':
        print("\n🗑️  Deleting accounts...")
        
        deleted = 0
        errors = 0
        
        # Delete in batches with progress
        batch_size = 200
        for i in range(0, len(accounts), batch_size):
            batch = accounts[i:i + batch_size]
            
            for acc in batch:
                try:
                    sf.Account.delete(acc['Id'])
                    deleted += 1
                except Exception as e:
                    errors += 1
                    print(f"❌ Error deleting {acc['Name']}: {str(e)[:100]}")
            
            # Progress update
            print(f"Progress: {min(i + batch_size, len(accounts)):,}/{len(accounts):,} accounts processed ({deleted:,} deleted, {errors} errors)")
        
        print("\n" + "=" * 80)
        print(f"✅ Deletion complete!")
        print(f"   Deleted: {deleted:,}")
        print(f"   Errors: {errors}")
        print("=" * 80)
    else:
        print("\n❌ Deletion cancelled")
else:
    print("\nNo accounts found matching this filter")

print("\nDone!")
