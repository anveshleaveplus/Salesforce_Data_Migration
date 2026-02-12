import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Load environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

# Connect to Salesforce
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain=os.getenv('SF_DOMAIN', 'test')
)

print("=" * 80)
print("Salesforce Contact.Union__c Field Metadata")
print("=" * 80)

# Get Contact object metadata
contact_metadata = sf.Contact.describe()

# Find Union__c field
union_field = None
for field in contact_metadata['fields']:
    if field['name'] == 'Union__c':
        union_field = field
        break

if union_field:
    print(f"\nField Name: {union_field['name']}")
    print(f"Type: {union_field['type']}")
    print(f"Label: {union_field['label']}")
    print(f"Length: {union_field.get('length', 'N/A')}")
    print(f"Custom: {union_field['custom']}")
    print(f"Updateable: {union_field['updateable']}")
    print(f"Createable: {union_field['createable']}")
    print(f"Nillable: {union_field['nillable']}")
    
    if union_field['type'] == 'picklist':
        print(f"\nPicklist Values:")
        for value in union_field.get('picklistValues', []):
            if value['active']:
                print(f"   - {value['value']}")
    
    if union_field['type'] == 'reference':
        print(f"\nReferences: {union_field.get('referenceTo', [])}")
else:
    print("\n[ERROR] Union__c field not found on Contact object")
    print("\nSearching for similar fields...")
    union_fields = [f['name'] for f in contact_metadata['fields'] if 'union' in f['name'].lower()]
    if union_fields:
        print(f"Found: {', '.join(union_fields)}")
    else:
        print("No fields with 'union' in the name found")

print("\n" + "=" * 80)
