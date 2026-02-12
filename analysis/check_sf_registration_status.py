"""
Check if Salesforce Contact.RegistrationStatus__c has all required picklist values
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
print("Checking Contact.RegistrationStatus__c picklist values")
print("="*80)

sf = connect_salesforce()

# Get Contact metadata
try:
    contact_metadata = sf.Contact.describe()
    
    # Find RegistrationStatus__c field
    reg_status_field = None
    for field in contact_metadata['fields']:
        if field['name'] == 'RegistrationStatus__c':
            reg_status_field = field
            break
    
    if not reg_status_field:
        print("\n[ERROR] RegistrationStatus__c field not found on Contact object!")
        print("        Please create this custom field in Salesforce first.")
        exit(1)
    
    print(f"\n✓ Field found: {reg_status_field['name']}")
    print(f"  Type: {reg_status_field['type']}")
    print(f"  Label: {reg_status_field['label']}")
    
    if reg_status_field['type'] != 'picklist':
        print(f"\n[WARNING] Field type is {reg_status_field['type']}, expected 'picklist'")
    
    # Get picklist values
    if 'picklistValues' in reg_status_field:
        print(f"\n  Current picklist values:")
        active_values = []
        for pv in reg_status_field['picklistValues']:
            status = "Active" if pv['active'] else "Inactive"
            print(f"    - {pv['value']} ({status})")
            if pv['active']:
                active_values.append(pv['value'])
        
        # Check required values from Oracle
        required_values = ['Unregistered', 'Registered', 'Suspended', 'Cancelled', 'Unknown']
        
        print(f"\n  Required values from Oracle (Code Set 12):")
        missing_values = []
        for req_val in required_values:
            if req_val in active_values:
                print(f"    ✓ {req_val} - Found")
            else:
                print(f"    ✗ {req_val} - MISSING")
                missing_values.append(req_val)
        
        if missing_values:
            print(f"\n[ERROR] Missing {len(missing_values)} required picklist value(s)!")
            print(f"        Please add these values to Salesforce:")
            for val in missing_values:
                print(f"          - {val}")
            exit(1)
        else:
            print(f"\n✓ All required picklist values are present in Salesforce")
    else:
        print("\n[ERROR] Could not retrieve picklist values")
        exit(1)

except Exception as e:
    print(f"\n[ERROR] Failed to check field: {e}")
    exit(1)

print("\n" + "="*80)
print("READY TO LOAD REGISTRATION STATUS")
print("="*80)
