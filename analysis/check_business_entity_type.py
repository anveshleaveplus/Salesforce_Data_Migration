"""
Check BusinessEntityType__c field on Account object
"""
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv('.env.sit')

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print('\n' + '='*70)
print('SALESFORCE ACCOUNT - BusinessEntityType__c Field')
print('='*70)

# Get field metadata
account_desc = sf.Account.describe()
field = next((f for f in account_desc['fields'] if f['name'] == 'BusinessEntityType__c'), None)

if field:
    print(f'\nField Type: {field["type"]}')
    print(f'Length: {field.get("length", "N/A")}')
    print(f'Required: {"Yes" if not field["nillable"] else "No"}')
    
    if field['type'] == 'picklist' and field.get('picklistValues'):
        print(f'\nPicklist Values ({len(field["picklistValues"])} total):')
        print('-'*70)
        for pv in field['picklistValues']:
            status = '✓' if pv['active'] else '✗'
            default = ' [DEFAULT]' if pv.get('defaultValue') else ''
            print(f'  {status} {pv["value"]}{default}')
    
    # Query for actual usage in SF
    print('\n' + '='*70)
    print('Actual Usage in Salesforce (Accounts with External_Id__c)')
    print('='*70)
    
    result = sf.query('''
        SELECT BusinessEntityType__c, COUNT(Id) cnt
        FROM Account
        WHERE External_Id__c != null
        GROUP BY BusinessEntityType__c
        ORDER BY COUNT(Id) DESC
    ''')
    
    total = sum(r['cnt'] for r in result['records'])
    print(f'\nTotal Accounts: {total:,}\n')
    
    for rec in result['records']:
        value = rec['BusinessEntityType__c'] or '(blank/null)'
        count = rec['cnt']
        pct = (count/total*100) if total > 0 else 0
        print(f'  {value:30s} {count:6,} ({pct:5.1f}%)')
else:
    print('\n❌ Field BusinessEntityType__c not found on Account object')

print('\n' + '='*70)
