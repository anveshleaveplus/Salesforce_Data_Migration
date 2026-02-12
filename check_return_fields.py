"""
Check Return__c object fields in Salesforce SIT
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
print("RETURN__C OBJECT FIELDS - SALESFORCE SIT")
print("=" * 100)
print(f"Environment: {os.getenv('SF_USERNAME')}\n")

# Get Return__c object metadata
return_metadata = sf.Return__c.describe()

print(f"Object: {return_metadata['label']} ({return_metadata['name']})")
print(f"Total Fields: {len(return_metadata['fields'])}\n")

print("=" * 100)
print("ALL FIELDS:")
print("=" * 100)
print(f"{'Field Name':<40} {'Type':<20} {'Label':<40} {'Required':<10}")
print("-" * 100)

# Sort fields by name
fields = sorted(return_metadata['fields'], key=lambda x: x['name'])

standard_fields = []
custom_fields = []

for field in fields:
    field_name = field['name']
    field_type = field['type']
    field_label = field['label']
    is_required = 'Yes' if not field['nillable'] and not field['defaultedOnCreate'] else 'No'
    
    # Check if custom field
    if field_name.endswith('__c') or field_name.endswith('__r'):
        custom_fields.append(field)
    else:
        standard_fields.append(field)
    
    print(f"{field_name:<40} {field_type:<20} {field_label:<40} {is_required:<10}")

print("\n" + "=" * 100)
print("CUSTOM FIELDS ONLY:")
print("=" * 100)
print(f"{'Field Name':<40} {'Type':<20} {'Label':<40} {'Length':<10}")
print("-" * 100)

for field in custom_fields:
    field_name = field['name']
    field_type = field['type']
    field_label = field['label']
    
    # Get length or precision info
    length_info = ''
    if field['length'] > 0:
        length_info = str(field['length'])
    elif field['precision'] > 0:
        length_info = f"{field['precision']},{field['scale']}"
    
    print(f"{field_name:<40} {field_type:<20} {field_label:<40} {length_info:<10}")
    
    # Show picklist values if applicable
    if field_type == 'picklist' and field.get('picklistValues'):
        print(f"  → Picklist values: {', '.join([v['value'] for v in field['picklistValues']])}")
    
    # Show reference info if lookup/master-detail
    if field_type == 'reference' and field.get('referenceTo'):
        print(f"  → References: {', '.join(field['referenceTo'])}")

print("\n" + "=" * 100)
print("RELATIONSHIP FIELDS:")
print("=" * 100)

for field in custom_fields:
    if field['type'] == 'reference':
        field_name = field['name']
        field_label = field['label']
        references = ', '.join(field['referenceTo'])
        relationship_name = field.get('relationshipName', 'N/A')
        
        print(f"  • {field_label} ({field_name})")
        print(f"    → References: {references}")
        print(f"    → Relationship Name: {relationship_name}")
        print()

print("=" * 100)
print("SAMPLE RECORDS:")
print("=" * 100)

# Query for sample records
query = """
    SELECT Id, Name, CreatedDate
    FROM Return__c
    ORDER BY CreatedDate DESC
    LIMIT 5
"""

results = sf.query(query)
if results['totalSize'] > 0:
    print(f"Found {results['totalSize']} total records. Showing first 5:\n")
    for record in results['records']:
        print(f"  • {record['Name']} (Created: {record['CreatedDate'][:10]})")
else:
    print("No records found.")

print("\n" + "=" * 100)
