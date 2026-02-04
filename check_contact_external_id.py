from dotenv import load_dotenv
import os
from simple_salesforce import Salesforce

env_file = '.env.sit'
load_dotenv(env_file)

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

desc = sf.Contact.describe()
external_fields = [f['name'] for f in desc['fields'] if f.get('externalId') == True]

print('External ID fields on Contact:')
for field in external_fields:
    print(f'  - {field}')

print('\nCustom fields with "ID" in name:')
id_fields = [f['name'] for f in desc['fields'] if f['custom'] and 'id' in f['name'].lower()]
for field in id_fields[:20]:
    print(f'  - {field}')

result = sf.query('SELECT COUNT() FROM Contact')
print(f'\nTotal Contacts in SIT: {result["totalSize"]}')
