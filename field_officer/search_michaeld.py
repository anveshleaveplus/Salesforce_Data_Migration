"""
Search for any Michael Docherty user variations
"""

import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv()

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("Searching for Michael Docherty users...\n")

# Search by name
users = sf.query("""
    SELECT Id, Username, FirstName, LastName, IsActive, Email
    FROM User
    WHERE (FirstName LIKE '%Michael%' AND LastName LIKE '%Docherty%')
    OR Username LIKE '%docherty%'
""")

if users['totalSize'] > 0:
    print(f"Found {users['totalSize']} user(s):\n")
    for user in users['records']:
        print(f"  {user['FirstName']} {user['LastName']}")
        print(f"  Username: {user['Username']}")
        print(f"  User ID: {user['Id']}")
        print(f"  Active: {user['IsActive']}")
        print(f"  Email: {user.get('Email', 'N/A')}")
        print()
else:
    print("No Michael Docherty users found in this org")
    print("\nThe duplicate error might be in a different Salesforce org.")
    print("Try using a different email like: michael.docherty2@leaveplus.com.au")
