"""
Delete LONG SERVICE LEAVE CREDITS (External_Id: 23000) from Salesforce SIT
"""
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import os

load_dotenv('.env.sit')

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain=os.getenv('SF_DOMAIN')
)

print("="*80)
print("DELETING LONG SERVICE LEAVE CREDITS (External_Id: 23000)")
print("="*80)

# First, find the record
query = """
SELECT Id, External_Id__c, Name, NumberOfEmployees
FROM Account
WHERE External_Id__c = '23000'
"""

result = sf.query(query)

if result['totalSize'] == 0:
    print("\n✓ Record not found - already deleted or doesn't exist")
else:
    rec = result['records'][0]
    record_id = rec['Id']
    
    print(f"\nFound record:")
    print(f"  Id: {record_id}")
    print(f"  External_Id__c: {rec['External_Id__c']}")
    print(f"  Name: {rec['Name']}")
    print(f"  NumberOfEmployees: {rec['NumberOfEmployees']:,}")
    
    # Delete the record
    print(f"\nDeleting record {record_id}...")
    try:
        sf.Account.delete(record_id)
        print("✓ Record deleted successfully")
        
        # Verify deletion
        verify = sf.query(query)
        if verify['totalSize'] == 0:
            print("✓ Deletion verified - record no longer exists")
        else:
            print("⚠ Warning: Record still exists after deletion")
            
    except Exception as e:
        print(f"✗ Error deleting record: {e}")

# Get updated count
total_count = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null")['totalSize']
print(f"\n" + "="*80)
print(f"Total Accounts in Salesforce: {total_count:,}")
print(f"Oracle active employers: 53,856")
print(f"Difference: {total_count - 53856:,}")
print("="*80)
