"""
Verify new Contact fields were loaded: UnionDelegate__c, Phone, OtherPhone
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

print("=" * 80)
print("VERIFY NEW CONTACT FIELDS LOADED")
print("=" * 80)

# Query contacts with new fields
query = """
    SELECT Id, External_Id__c, FirstName, LastName, 
           UnionDelegate__c, Phone, OtherPhone, MobilePhone
    FROM Contact 
    WHERE External_Id__c != null
    ORDER BY LastModifiedDate DESC
    LIMIT 10
"""

print("\n[1] Recent Contacts with New Fields:")
print("-" * 80)

result = sf.query(query)

if result['totalSize'] > 0:
    print(f"Found {result['totalSize']} contacts\n")
    
    union_delegate_true = 0
    phone_count = 0
    other_phone_count = 0
    mobile_count = 0
    
    for record in result['records']:
        print(f"External_Id: {record['External_Id__c']}")
        print(f"  Name: {record['FirstName']} {record['LastName']}")
        print(f"  UnionDelegate__c: {record.get('UnionDelegate__c', 'NULL')}")
        print(f"  Phone: {record.get('Phone', 'NULL')}")
        print(f"  OtherPhone: {record.get('OtherPhone', 'NULL')}")
        print(f"  MobilePhone: {record.get('MobilePhone', 'NULL')}")
        print()
        
        if record.get('UnionDelegate__c'):
            union_delegate_true += 1
        if record.get('Phone'):
            phone_count += 1
        if record.get('OtherPhone'):
            other_phone_count += 1
        if record.get('MobilePhone'):
            mobile_count += 1
    
    print(f"Sample Coverage (10 records):")
    print(f"  UnionDelegate__c = True: {union_delegate_true}")
    print(f"  Phone populated: {phone_count}")
    print(f"  OtherPhone populated: {other_phone_count}")
    print(f"  MobilePhone populated: {mobile_count}")

# Get overall statistics
print("\n[2] Overall Field Coverage:")
print("-" * 80)

# UnionDelegate__c coverage
query = """
    SELECT COUNT(Id) total_contacts,
           COUNT_IF(UnionDelegate__c = true) union_delegates
    FROM Contact
    WHERE External_Id__c != null
"""

try:
    result = sf.query(query)
    if result['totalSize'] > 0:
        record = result['records'][0]
        total = record.get('total_contacts', 0)
        delegates = record.get('union_delegates', 0)
        print(f"UnionDelegate__c coverage:")
        print(f"  Total contacts: {total:,}")
        print(f"  Union delegates (True): {delegates:,}")
        if total > 0:
            print(f"  Percentage: {delegates/total*100:.2f}%")
except Exception as e:
    print(f"Note: Aggregate query not supported, using alternative method")
    
    # Alternative: Count using regular queries
    total_query = "SELECT COUNT() FROM Contact WHERE External_Id__c != null"
    total = sf.query(total_query)['totalSize']
    
    delegate_query = "SELECT COUNT() FROM Contact WHERE External_Id__c != null AND UnionDelegate__c = true"
    delegates = sf.query(delegate_query)['totalSize']
    
    print(f"UnionDelegate__c coverage:")
    print(f"  Total contacts: {total:,}")
    print(f"  Union delegates (True): {delegates:,}")
    if total > 0:
        print(f"  Percentage: {delegates/total*100:.2f}%")

# Phone field coverage
phone_query = "SELECT COUNT() FROM Contact WHERE External_Id__c != null AND Phone != null"
phone_count = sf.query(phone_query)['totalSize']

other_phone_query = "SELECT COUNT() FROM Contact WHERE External_Id__c != null AND OtherPhone != null"
other_phone_count = sf.query(other_phone_query)['totalSize']

mobile_query = "SELECT COUNT() FROM Contact WHERE External_Id__c != null AND MobilePhone != null"
mobile_count = sf.query(mobile_query)['totalSize']

print(f"\nPhone field coverage (out of {total:,} contacts):")
print(f"  Phone: {phone_count:,} ({phone_count/total*100:.2f}%)")
print(f"  OtherPhone: {other_phone_count:,} ({other_phone_count/total*100:.2f}%)")
print(f"  MobilePhone: {mobile_count:,} ({mobile_count/total*100:.2f}%)")

print("\n" + "=" * 80)
print("VERIFICATION COMPLETE")
print("=" * 80)
print("\n✅ New fields are loaded:" if union_delegates > 0 or phone_count > 0 else "\n❌ New fields NOT loaded")
print(f"   - UnionDelegate__c: {'✓' if delegates > 0 else '✗'}")
print(f"   - Phone: {'✓' if phone_count > 0 else '✗'}")
print(f"   - OtherPhone: {'✓' if other_phone_count > 0 else '✗'}")
