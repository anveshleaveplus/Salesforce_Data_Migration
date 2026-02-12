"""
Clear all Contact address fields in Salesforce SIT
Use this to reset addresses before reloading with new mapping
"""

import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv('.env.sit')

def connect_salesforce():
    """Establish Salesforce connection"""
    return Salesforce(
        username=os.getenv('SF_USERNAME'),
        password=os.getenv('SF_PASSWORD'),
        security_token=os.getenv('SF_SECURITY_TOKEN'),
        domain=os.getenv('SF_DOMAIN', 'test')
    )

print("="*80)
print("CLEAR CONTACT ADDRESSES")
print("="*80)

sf = connect_salesforce()

# Query all Contacts with addresses
query = """
    SELECT Id 
    FROM Contact 
    WHERE External_Id__c != NULL
    AND (MailingStreet != NULL OR OtherStreet != NULL)
"""

print(f"\nQuerying Contacts with addresses...")
result = sf.query_all(query)
contacts = result['records']
print(f"Found {len(contacts):,} Contacts with addresses")

if len(contacts) == 0:
    print("No addresses to clear.")
    exit(0)

# Prepare update payload to clear addresses
updates = []
for contact in contacts:
    updates.append({
        'Id': contact['Id'],
        'MailingStreet': None,
        'MailingCity': None,
        'MailingState': None,
        'MailingPostalCode': None,
        'MailingCountry': None,
        'OtherStreet': None,
        'OtherCity': None,
        'OtherState': None,
        'OtherPostalCode': None,
        'OtherCountry': None
    })

# Update in batches
print(f"\nClearing addresses from {len(updates):,} Contacts...")
batch_size = 200
success_count = 0
error_count = 0

for i in range(0, len(updates), batch_size):
    batch = updates[i:i+batch_size]
    try:
        result = sf.bulk.Contact.update(batch)
        for item in result:
            if item['success']:
                success_count += 1
            else:
                error_count += 1
        
        if (i + batch_size) % 1000 == 0:
            print(f"  Progress: {i + batch_size:,}/{len(updates):,} ({success_count:,} success, {error_count} errors)")
    except Exception as e:
        print(f"  [ERROR] Batch {i}-{i+batch_size}: {e}")
        error_count += len(batch)

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Successfully cleared: {success_count:,} Contacts")
print(f"Errors: {error_count}")
print("="*80)
