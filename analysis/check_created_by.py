from dotenv import load_dotenv
import os
from simple_salesforce import Salesforce

load_dotenv('.env.sit')
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

# Get current user ID
user_info = sf.query(f"SELECT Id, Name, Username FROM User WHERE Username = '{os.getenv('SF_USERNAME')}'")
if user_info['records']:
    user = user_info['records'][0]
    print(f"Current User: {user['Name']} ({user['Username']})")
    print(f"User ID: {user['Id']}")
    print()
    
    # Count accounts created by this user
    result = sf.query(f"SELECT COUNT() FROM Account WHERE CreatedById = '{user['Id']}'")
    count = result['totalSize']
    print(f"Accounts created by you: {count:,}")
    
    if count > 0:
        print(f"\n✅ Can delete {count:,} accounts using:")
        print(f"   WHERE CreatedById = '{user['Id']}'")
        print("\nThis is much faster - no need to check External_Id__c!")
else:
    print("Could not find current user")
