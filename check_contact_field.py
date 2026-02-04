"""
Check if Contact.External_Id__c field exists in SIT
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

print("\nChecking Contact object for External_Id__c field...")

try:
    # Try to query the field
    result = sf.query("SELECT External_Id__c FROM Contact LIMIT 1")
    print("✅ External_Id__c field EXISTS on Contact object")
    print(f"   Contact count: {sf.query('SELECT COUNT() FROM Contact')['totalSize']:,}")
    print("\n✅ Ready to proceed with contact load!")
except Exception as e:
    if "No such column 'External_Id__c'" in str(e):
        print("❌ External_Id__c field MISSING on Contact object")
        print("\n⚠️  Cannot proceed with contact load until field is created.")
        print("\nRequired field specifications:")
        print("  • Field Name: External_Id__c")
        print("  • Type: Text(18)")
        print("  • Unique: Yes")
        print("  • External ID: Yes")
        print("  • Case Sensitive: No")
        print("\nNotification sent: CONTACT_EXTERNAL_ID_MISSING.md")
    else:
        print(f"❌ Error: {e}")
