"""
Check if new Salesforce Account fields exist before loading data
"""

import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv('.env.sit')

print("\n" + "="*80)
print("SALESFORCE ACCOUNT FIELD VERIFICATION")
print("="*80)

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

account_desc = sf.Account.describe()

required_fields = {
    'ABNRegistrationDate__c': {'type': 'date', 'required_for': 'SQL Server ABR data'},
    'AccountStatus__c': {'type': 'picklist', 'required_for': 'SQL Server ABR status'},
    'Classifications__c': {'type': 'picklist', 'required_for': 'SQL Server industry class'},
    'OSCACode__c': {'type': 'string', 'required_for': 'SQL Server industry code'}
}

print("\nChecking new fields:")
print("-" * 80)

all_exist = True
for field_name, info in required_fields.items():
    field = next((f for f in account_desc['fields'] if f['name'] == field_name), None)
    
    if field:
        status = "✅ EXISTS"
        type_info = f"{field['type']}"
        if field['length']:
            type_info += f"({field['length']})"
        
        print(f"  {status} {field_name:30s} {type_info:20s} - {info['required_for']}")
        
        # Check picklist values
        if field['type'] == 'picklist' and field.get('picklistValues'):
            values = [v['value'] for v in field['picklistValues'] if v['active']]
            print(f"        Picklist values ({len(values)}): {', '.join(values[:5])}")
            if len(values) > 5:
                print(f"        ... and {len(values) - 5} more")
    else:
        status = "❌ MISSING"
        all_exist = False
        print(f"  {status} {field_name:30s} - {info['required_for']}")
        print(f"        Expected type: {info['type']}")

print("\n" + "="*80)

if all_exist:
    print("✅ ALL REQUIRED FIELDS EXIST - Ready to load data!")
    print("\nYou can run: python sit_account_load.py")
else:
    print("❌ MISSING FIELDS - Cannot load data yet")
    print("\nPlease create missing fields in Salesforce Setup:")
    print("  1. Setup > Object Manager > Account")
    print("  2. Fields & Relationships > New")
    print("  3. Create the missing fields listed above")
    print("\nFor picklist fields, add these values:")
    print("  - AccountStatus__c: Active, Cancelled")
    print("  - Classifications__c: (All ANZSIC industry class values)")

print("="*80)
