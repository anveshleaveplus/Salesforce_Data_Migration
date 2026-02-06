"""
SIT - Delete existing test Account records (using standard API)
Removes the 10,000 test accounts loaded previously
Uses standard REST API instead of Bulk API for reliability
"""

from dotenv import load_dotenv
import os
from simple_salesforce import Salesforce

# Load SIT environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

print("="*70)
print("SIT - Delete Existing Test Accounts (Fast Method)")
print("="*70)

# Connect to Salesforce
print("\n[1/3] Connecting to Salesforce SIT...")
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)
print("      [OK] Connected")

# Fetch all account IDs
print("\n[2/3] Fetching Account IDs...")
account_ids = []
query = "SELECT Id FROM Account WHERE External_Id__c != null"
result = sf.query(query)

while True:
    account_ids.extend([rec['Id'] for rec in result['records']])
    if result['done']:
        break
    result = sf.query_more(result['nextRecordsUrl'], identifier_is_url=True)

print(f"      Retrieved {len(account_ids):,} Account IDs")

if len(account_ids) == 0:
    print("\n[OK] No accounts to delete - already clean!")
    exit(0)

# Delete using composite API (faster, more reliable)
print(f"\n[3/3] Deleting {len(account_ids):,} accounts...")
print("      Using Composite API in batches of 200...")

BATCH_SIZE = 200
deleted_count = 0
error_count = 0
errors = []

for i in range(0, len(account_ids), BATCH_SIZE):
    batch = account_ids[i:i+BATCH_SIZE]
    batch_num = (i // BATCH_SIZE) + 1
    total_batches = (len(account_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    
    try:
        # Delete using REST API (one at a time in batch)
        for account_id in batch:
            try:
                sf.Account.delete(account_id)
                deleted_count += 1
            except Exception as e:
                error_count += 1
                errors.append({'id': account_id, 'error': str(e)})
        
        # Progress every 10 batches or at end
        if batch_num % 10 == 0 or batch_num == total_batches:
            print(f"      Progress: {batch_num}/{total_batches} batches ({deleted_count:,} deleted, {error_count} errors)")
            
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
print(f"Original count:     {len(account_ids):,}")
print(f"Deleted:            {deleted_count:,}")
print(f"Errors:             {error_count:,}")
print(f"Remaining:          {remaining:,}")

if error_count > 0 and len(errors) <= 10:
    print(f"\nSample errors:")
    for err in errors[:10]:
        print(f"  {err['id']}: {err['error']}")

if remaining == 0:
    print("\n✅ [SUCCESS] All accounts deleted successfully!")
else:
    print(f"\n⚠️  [WARNING] {remaining:,} accounts still remain")

print("\n[OK] Cleanup complete")
