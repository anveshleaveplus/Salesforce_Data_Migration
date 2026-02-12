"""
Check if specific workers from Oracle 50K were actually updated
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

print("Checking Oracle workers in Salesforce...")

# Check a few specific External IDs that should be in the 50K load
test_ids = ['100', '200', '500', '1000', '5000', '10000']

for ext_id in test_ids:
    query = f"""
        SELECT Id, External_Id__c, FirstName, LastName, 
               UnionDelegate__c, Phone, OtherPhone, MobilePhone,
               LastModifiedDate
        FROM Contact
        WHERE External_Id__c = '{ext_id}'
    """
    
    result = sf.query(query)
    
    if result['totalSize'] > 0:
        rec = result['records'][0]
        print(f"\n✓ Found External_Id: {ext_id}")
        print(f"  Name: {rec.get('FirstName')} {rec.get('LastName')}")
        print(f"  UnionDelegate__c: {rec.get('UnionDelegate__c')}")
        print(f"  Phone: {rec.get('Phone')}")
        print(f"  LastModified: {rec.get('LastModifiedDate')}")
    else:
        print(f"\n✗ NOT FOUND: External_Id {ext_id}")

print("\n" + "="*60)
print("If contacts are NOT FOUND, the load did not execute.")
print("If contacts found but fields NULL, load ran but failed.")
