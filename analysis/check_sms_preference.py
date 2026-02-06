"""
Check IS_SMS_DISABLED field - communication preference
"""
import os
import oracledb
from dotenv import load_dotenv

load_dotenv('.env.sit')

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print('\n' + '='*80)
print('IS_SMS_DISABLED - Communication Preference Analysis')
print('='*80)

# Check value distribution for active employers
cursor.execute("""
    SELECT 
        c.IS_SMS_DISABLED,
        COUNT(*) as employer_count
    FROM SCH_CO_20.CO_CUSTOMER c
    WHERE c.CUSTOMER_ID IN (
        SELECT DISTINCT e.CUSTOMER_ID
        FROM SCH_CO_20.CO_EMPLOYER e
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            INNER JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep ON ep.WORKER_ID = s.WORKER
            WHERE ep.EMPLOYER_ID = e.CUSTOMER_ID
            AND s.PERIOD_END >= 202301
        )
    )
    GROUP BY c.IS_SMS_DISABLED
    ORDER BY COUNT(*) DESC
""")

print('\nIS_SMS_DISABLED value distribution (Active Employers):')
print('-'*80)

results = cursor.fetchall()
total = sum(r[1] for r in results)

for value, count in results:
    value_str = str(value) if value else 'NULL'
    pct = (count/total*100) if total > 0 else 0
    print(f'  {value_str:15s} {count:7,} ({pct:5.1f}%)')

print(f'\nTotal: {total:,}')

# Get sample employers with SMS disabled
print('\n' + '='*80)
print('Sample Employers with SMS DISABLED (IS_SMS_DISABLED = Y or 1)')
print('='*80)

cursor.execute("""
    SELECT 
        e.CUSTOMER_ID,
        e.TRADING_NAME,
        c.IS_SMS_DISABLED,
        c.EMAIL_ADDRESS,
        c.MOBILE_PHONE_NO
    FROM SCH_CO_20.CO_EMPLOYER e
    INNER JOIN SCH_CO_20.CO_CUSTOMER c ON c.CUSTOMER_ID = e.CUSTOMER_ID
    WHERE c.IS_SMS_DISABLED IN ('Y', '1')
    AND e.CUSTOMER_ID IN (
        SELECT DISTINCT ep.EMPLOYER_ID
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= 202301
        )
    )
    AND ROWNUM <= 10
    ORDER BY e.CUSTOMER_ID
""")

sms_disabled = cursor.fetchall()
if sms_disabled:
    for row in sms_disabled:
        print(f'\nCUSTOMER_ID: {row[0]}')
        print(f'  Name: {row[1]}')
        print(f'  IS_SMS_DISABLED: {row[2]}')
        print(f'  Email: {row[3] or "NULL"}')
        print(f'  Mobile: {row[4] or "NULL"}')
else:
    print('  No employers with SMS disabled')

# Check Salesforce for matching field
print('\n' + '='*80)
print('Salesforce Account Object - Communication Preference Fields')
print('='*80)

from simple_salesforce import Salesforce

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

account_desc = sf.Account.describe()

# Look for SMS or communication preference fields
comm_fields = [f for f in account_desc['fields'] 
               if 'sms' in f['name'].lower() 
               or 'communication' in f['name'].lower()
               or 'preference' in f['name'].lower()
               or 'consent' in f['name'].lower()
               or 'opt' in f['name'].lower()]

if comm_fields:
    print('\nCommunication preference fields found:')
    for field in comm_fields:
        print(f'  {field["name"]:40s} {field["type"]:15s} (Nullable: {field["nillable"]})')
else:
    print('\n⚠️  No SMS/communication preference fields found in Account object')
    print('    Recommended field to create:')
    print('    • Field Name: IsSMSDisabled__c or DoNotContactViaSMS__c')
    print('    • Type: Checkbox (Boolean)')
    print('    • Description: Indicates if employer opted out of SMS communications')

cursor.close()
conn.close()

print('\n' + '='*80)
