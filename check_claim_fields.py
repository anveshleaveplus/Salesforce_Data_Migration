"""
Check Claim__c and ClaimComponent__c object fields in Salesforce SIT
"""
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Load SIT environment
load_dotenv('.env.sit')

# Connect to Salesforce
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("=" * 100)
print("CLAIM__C OBJECT FIELDS - SALESFORCE SIT")
print("=" * 100)
print(f"Environment: {os.getenv('SF_USERNAME')}\n")

# Get Claim__c object metadata
claim_metadata = sf.Claim__c.describe()

print(f"Object: {claim_metadata['label']} ({claim_metadata['name']})")
print(f"Total Fields: {len(claim_metadata['fields'])}\n")

# Get custom fields only
custom_fields = [f for f in claim_metadata['fields'] if f['name'].endswith('__c') or f['name'].endswith('__r')]

print("=" * 100)
print(f"CUSTOM FIELDS ({len(custom_fields)} total):")
print("=" * 100)
print(f"{'Field Name':<45} {'Type':<20} {'Label':<40}")
print("-" * 100)

for field in sorted(custom_fields, key=lambda x: x['name']):
    field_name = field['name']
    field_type = field['type']
    field_label = field['label']
    
    print(f"{field_name:<45} {field_type:<20} {field_label:<40}")
    
    # Show reference info if lookup/master-detail
    if field_type == 'reference' and field.get('referenceTo'):
        print(f"  → References: {', '.join(field['referenceTo'])}")
    
    # Show picklist values if applicable
    if field_type == 'picklist' and field.get('picklistValues'):
        values = [v['value'] for v in field['picklistValues'][:5]]  # First 5
        more = f" (+ {len(field['picklistValues']) - 5} more)" if len(field['picklistValues']) > 5 else ""
        print(f"  → Values: {', '.join(values)}{more}")

# Query for sample records
print("\n" + "=" * 100)
print("SAMPLE CLAIM RECORDS:")
print("=" * 100)

query = "SELECT Id, Name, CreatedDate FROM Claim__c ORDER BY CreatedDate DESC LIMIT 5"
results = sf.query(query)
print(f"Total Claims: {results['totalSize']}\n")
if results['totalSize'] > 0:
    for record in results['records']:
        print(f"  • {record['Name']} (Created: {record['CreatedDate'][:10]})")
else:
    print("No claims found.")

# Now check ClaimComponent__c
print("\n\n" + "=" * 100)
print("CLAIMCOMPONENT__C OBJECT FIELDS - SALESFORCE SIT")
print("=" * 100)

component_metadata = sf.ClaimComponent__c.describe()
print(f"Object: {component_metadata['label']} ({component_metadata['name']})")
print(f"Total Fields: {len(component_metadata['fields'])}\n")

# Get custom fields only
custom_fields_comp = [f for f in component_metadata['fields'] if f['name'].endswith('__c') or f['name'].endswith('__r')]

print("=" * 100)
print(f"CUSTOM FIELDS ({len(custom_fields_comp)} total):")
print("=" * 100)
print(f"{'Field Name':<45} {'Type':<20} {'Label':<40}")
print("-" * 100)

for field in sorted(custom_fields_comp, key=lambda x: x['name']):
    field_name = field['name']
    field_type = field['type']
    field_label = field['label']
    
    print(f"{field_name:<45} {field_type:<20} {field_label:<40}")
    
    # Show reference info if lookup/master-detail
    if field_type == 'reference' and field.get('referenceTo'):
        print(f"  → References: {', '.join(field['referenceTo'])}")
    
    # Show picklist values if applicable
    if field_type == 'picklist' and field.get('picklistValues'):
        values = [v['value'] for v in field['picklistValues'][:5]]
        more = f" (+ {len(field['picklistValues']) - 5} more)" if len(field['picklistValues']) > 5 else ""
        print(f"  → Values: {', '.join(values)}{more}")

# Query for sample records
print("\n" + "=" * 100)
print("SAMPLE CLAIM COMPONENT RECORDS:")
print("=" * 100)

query = "SELECT Id, Name, CreatedDate FROM ClaimComponent__c ORDER BY CreatedDate DESC LIMIT 5"
results = sf.query(query)
print(f"Total Claim Components: {results['totalSize']}\n")
if results['totalSize'] > 0:
    for record in results['records']:
        print(f"  • {record['Name']} (Created: {record['CreatedDate'][:10]})")
else:
    print("No claim components found.")

print("\n" + "=" * 100)
