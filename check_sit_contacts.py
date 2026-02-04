"""
Quick check - how many contacts loaded to SIT
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
print("SIT CONTACT STATUS CHECK")
print("="*70)

# Total contacts
total = sf.query("SELECT COUNT() FROM Contact")['totalSize']
print(f"\nTotal Contacts: {total:,}")

# Contacts with External_Id__c
with_ext_id = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != null")['totalSize']
print(f"Contacts with External_Id__c: {with_ext_id:,}")

# Total ACRs
acr_count = sf.query("SELECT COUNT() FROM AccountContactRelation")['totalSize']
print(f"Total AccountContactRelations: {acr_count:,}")

if with_ext_id > 0:
    # Sample contact
    sample = sf.query("SELECT Id, Name, External_Id__c, Email, AccountId FROM Contact WHERE External_Id__c != null LIMIT 1")
    if sample['totalSize'] > 0:
        rec = sample['records'][0]
        print(f"\nSample Contact:")
        print(f"  Name: {rec.get('Name')}")
        print(f"  External_Id__c: {rec.get('External_Id__c')}")
        print(f"  Email: {rec.get('Email', 'None')}")
        print(f"  AccountId: {rec.get('AccountId', 'None')}")

print("\n" + "="*70)
