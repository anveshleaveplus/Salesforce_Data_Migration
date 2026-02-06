"""
Delete SIT accounts created by 'Data admin' user
Uses REST API for reliable deletion
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
print("SIT Account Deletion - Data Admin Creator")
print("=" * 50)

# Query for accounts created by 'Data admin'
query = """
SELECT Id, Name, External_Id__c, CreatedBy.Name, CreatedDate
FROM Account
WHERE CreatedBy.Name = 'Data admin'
ORDER BY CreatedDate DESC
"""

print(f"\nQuerying accounts created by 'Data admin'...")
result = sf.query_all(query)
total = result['totalSize']

print(f"✅ Found {total:,} accounts")

if total == 0:
    print("No accounts to delete")
    exit()

print(f"\n⚠️  This will delete {total:,} accounts")
confirmation = input("Type 'DELETE' to confirm: ")

if confirmation != 'DELETE':
    print("❌ Deletion cancelled")
    exit()

# Delete accounts using REST API (one at a time)
print(f"\n🔄 Deleting {total:,} accounts...")
print("(This may take a while - REST API processes one at a time)")

deleted = 0
errors = 0
error_log = []

for record in result['records']:
    try:
        sf.Account.delete(record['Id'])
        deleted += 1
        if deleted % 100 == 0:
            print(f"Progress: {deleted:,}/{total:,} deleted ({deleted*100//total}%)")
    except Exception as e:
        errors += 1
        error_log.append({
            'Id': record['Id'],
            'Name': record.get('Name'),
            'Error': str(e)
        })
        if errors <= 10:  # Print first 10 errors
            print(f"❌ Error deleting {record.get('Name')}: {e}")

print(f"\n{'='*50}")
print(f"✅ Deleted: {deleted:,}")
if errors > 0:
    print(f"❌ Errors: {errors:,}")
    
    # Save error log
    if error_log:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_file = f'error/sit_delete_errors_{timestamp}.csv'
        os.makedirs('error', exist_ok=True)
        
        import csv
        with open(error_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['Id', 'Name', 'Error'])
            writer.writeheader()
            writer.writerows(error_log)
        print(f"📝 Error log saved to: {error_file}")

print("\nDone!")
