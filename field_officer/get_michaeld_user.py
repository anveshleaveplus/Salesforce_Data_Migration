"""
Get MICHAELD user ID (already exists) and add to mapping
"""

import os
import csv
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv()

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("Querying for existing MICHAELD user...")

existing = sf.query("SELECT Id, Username, FirstName, LastName FROM User WHERE Username = 'michael.docherty@leaveplus.com.au'")

if existing['totalSize'] > 0:
    user = existing['records'][0]
    print(f"✅ Found: {user['FirstName']} {user['LastName']} ({user['Username']})")
    print(f"   User ID: {user['Id']}")
    
    # Read existing mapping
    mapping = []
    with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        mapping = list(reader)
    
    # Add MICHAELD
    mapping.insert(0, {
        'Officer_Code': 'MICHAELD',
        'Salesforce_User_Id': user['Id'],
        'Email': user['Username'],
        'Status': 'Already Exists'
    })
    
    # Write back
    with open('field_officer_salesforce_mapping.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['Officer_Code', 'Salesforce_User_Id', 'Email', 'Status'])
        writer.writeheader()
        writer.writerows(mapping)
    
    print("✅ Updated field_officer_salesforce_mapping.csv")
else:
    print("❌ User not found")
