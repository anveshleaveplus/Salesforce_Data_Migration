"""
Check CommunicationPreference__c picklist values
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

print('\n' + '='*80)
print('CommunicationPreference__c Picklist Values')
print('='*80)

account_desc = sf.Account.describe()
field = next((f for f in account_desc['fields'] if f['name'] == 'CommunicationPreference__c'), None)

if field and field.get('picklistValues'):
    print(f'\nField Type: {field["type"]}')
    print(f'Required: {"Yes" if not field["nillable"] else "No"}')
    print(f'\nPicklist Values:')
    for pv in field['picklistValues']:
        status = '✓' if pv['active'] else '✗'
        default = ' [DEFAULT]' if pv.get('defaultValue') else ''
        print(f'  {status} {pv["value"]}{default}')
    
    # Check current usage
    print('\n' + '='*80)
    print('Current Usage in Salesforce')
    print('='*80)
    
    result = sf.query('''
        SELECT CommunicationPreference__c, COUNT(Id) cnt
        FROM Account
        WHERE External_Id__c != null
        GROUP BY CommunicationPreference__c
        ORDER BY COUNT(Id) DESC
    ''')
    
    total = sum(r['cnt'] for r in result['records'])
    print(f'\nTotal Accounts: {total:,}\n')
    
    for rec in result['records']:
        value = rec['CommunicationPreference__c'] or '(blank/null)'
        count = rec['cnt']
        pct = (count/total*100) if total > 0 else 0
        print(f'  {value:30s} {count:6,} ({pct:5.1f}%)')
else:
    print('Field info not available')

print('\n' + '='*80)
print('MAPPING RECOMMENDATION')
print('='*80)
print('\nOracle CO_CUSTOMER.IS_SMS_DISABLED → Salesforce Account.CommunicationPreference__c')
print('\nValues found in Oracle:')
print('  • N  (53,830 employers, 99.9%) - SMS communications enabled')
print('  • Y  (27 employers, 0.1%)      - SMS communications disabled')
print('\nSuggested mapping:')
print('  • If picklist has "SMS" or "Do Not Contact via SMS" → use that value for Y')
print('  • If picklist has "Email and SMS" or "All Channels" → use for N')
print('  • Otherwise, may need to add new picklist value')
print('='*80)
