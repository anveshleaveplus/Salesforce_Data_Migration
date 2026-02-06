"""
SIT - Delete existing test Account records
Removes the 10,000 test accounts loaded previously
"""

from dotenv import load_dotenv
import os
from simple_salesforce import Salesforce

# Load SIT environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)
print(f"Using environment: {env_file}\n")

print("="*70)
print("SIT - Delete Existing Test Accounts")
print("="*70)

# Connect to Salesforce
print("\n[1/4] Connecting to Salesforce SIT...")
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)
print("      [OK] Connected")

# Count existing accounts
print("\n[2/4] Counting existing accounts with External_Id__c...")
result = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null")
count = result['totalSize']
print(f"      Found {count:,} accounts to delete")

if count == 0:
    print("\n[OK] No accounts to delete - already clean!")
    exit(0)

# Confirm deletion
print(f"\n⚠️  WARNING: This will DELETE {count:,} Account records!")
print("      This action cannot be undone.")
response = input("\nType 'DELETE' to confirm: ")

if response != 'DELETE':
    print("\n[CANCELLED] Deletion aborted")
    exit(0)

# Fetch all account IDs
print("\n[3/4] Fetching Account IDs...")
account_ids = []
query = "SELECT Id FROM Account WHERE External_Id__c != null"
result = sf.query(query)

while True:
    account_ids.extend([rec['Id'] for rec in result['records']])
    if result['done']:
        break
    result = sf.query_more(result['nextRecordsUrl'], identifier_is_url=True)

print(f"      Retrieved {len(account_ids):,} Account IDs")

# Delete in batches
print("\n[4/4] Deleting accounts in batches of 200...")
BATCH_SIZE = 200
deleted_count = 0
error_count = 0

for i in range(0, len(account_ids), BATCH_SIZE):
    batch = account_ids[i:i+BATCH_SIZE]
    batch_num = (i // BATCH_SIZE) + 1
    total_batches = (len(account_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    
    try:
        # Delete batch
        results = sf.bulk.Account.delete(batch)
        
        # Count successes/errors
        for result in results:
            if result['success']:
                deleted_count += 1
            else:
                error_count += 1
                print(f"      Error deleting {result['id']}: {result['errors']}")
        
        # Progress
        if batch_num % 10 == 0 or batch_num == total_batches:
            print(f"      Progress: {batch_num}/{total_batches} batches ({deleted_count:,} deleted)")
            
    except Exception as e:
        print(f"      [ERROR] Batch {batch_num} failed: {e}")
        error_count += len(batch)

# Verify deletion
print("\n[VERIFY] Checking remaining accounts...")
result = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null")
remaining = result['totalSize']

print("\n" + "="*70)
print("DELETION SUMMARY")
print("="*70)
print(f"Original count:     {count:,}")
print(f"Deleted:            {deleted_count:,}")
print(f"Errors:             {error_count:,}")
print(f"Remaining:          {remaining:,}")

if remaining == 0:
    print("\n✅ [SUCCESS] All accounts deleted successfully!")
else:
    print(f"\n⚠️  [WARNING] {remaining:,} accounts still remain")
    print("    You may need to manually delete related records first (Contacts, etc.)")

print("\n[OK] Cleanup complete - ready for fresh load")
