import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain=os.getenv('SF_DOMAIN', 'test')
)

print("Searching for Title/Gender related fields on Contact object...\n")

contact_metadata = sf.Contact.describe()

print("Fields containing 'Title':")
for field in contact_metadata['fields']:
    if 'title' in field['name'].lower():
        print(f"   {field['name']} ({field['type']}) - {field['label']}")

print("\nFields containing 'Gender':")
for field in contact_metadata['fields']:
    if 'gender' in field['name'].lower():
        print(f"   {field['name']} ({field['type']}) - {field['label']}")

print("\nFields containing 'Salutation':")
for field in contact_metadata['fields']:
    if 'salutation' in field['name'].lower():
        print(f"   {field['name']} ({field['type']}) - {field['label']}")

print("\nFields containing 'Type':")
for field in contact_metadata['fields']:
    if 'type' in field['name'].lower() and field['name'] != 'RecordTypeId':
        print(f"   {field['name']} ({field['type']}) - {field['label']}")
