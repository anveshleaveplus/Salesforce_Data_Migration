"""
Find communication preference related columns in Oracle
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

# Search for communication/preference related columns
cursor.execute("""
    SELECT table_name, column_name, data_type
    FROM all_tab_columns
    WHERE owner = 'SCH_CO_20'
      AND (
        UPPER(column_name) LIKE '%COMM%'
        OR UPPER(column_name) LIKE '%PREFER%'
        OR UPPER(column_name) LIKE '%CONTACT%METHOD%'
        OR UPPER(column_name) LIKE '%CONSENT%'
        OR UPPER(column_name) LIKE '%OPT%'
        OR UPPER(column_name) LIKE '%SUBSCRI%'
        OR UPPER(column_name) LIKE '%NOTIFICATION%'
        OR UPPER(column_name) LIKE '%ALERT%'
      )
    ORDER BY table_name, column_name
""")

print('\n' + '='*80)
print('COMMUNICATION PREFERENCE RELATED COLUMNS IN ORACLE')
print('='*80)

results = cursor.fetchall()
if results:
    current_table = None
    for row in results:
        table_name, column_name, data_type = row
        if table_name != current_table:
            print(f'\n{table_name}:')
            current_table = table_name
        print(f'  {column_name:45s} {data_type}')
    print(f'\n\nTotal columns found: {len(results)}')
else:
    print('\nNo communication preference columns found')

# Check specific tables for employer/worker communication preferences
print('\n' + '='*80)
print('CHECKING CO_CUSTOMER TABLE FOR COMMUNICATION FLAGS')
print('='*80)

cursor.execute("""
    SELECT column_name, data_type
    FROM all_tab_columns
    WHERE owner = 'SCH_CO_20'
      AND table_name = 'CO_CUSTOMER'
    ORDER BY column_name
""")

print('\nAll CO_CUSTOMER columns:')
for row in cursor.fetchall():
    print(f'  {row[0]:45s} {row[1]}')

# Sample data from CO_CUSTOMER
print('\n' + '='*80)
print('SAMPLE DATA FROM CO_CUSTOMER (first 10 active employers)')
print('='*80)

cursor.execute("""
    SELECT 
        c.CUSTOMER_ID,
        c.EMAIL_ADDRESS,
        c.MOBILE_PHONE_NO,
        c.TELEPHONE1_NO
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
    AND ROWNUM <= 10
    ORDER BY c.CUSTOMER_ID
""")

for row in cursor.fetchall():
    print(f'\nCUSTOMER_ID: {row[0]}')
    print(f'  Email: {row[1] or "NULL"}')
    print(f'  Mobile: {row[2] or "NULL"}')
    print(f'  Phone: {row[3] or "NULL"}')

cursor.close()
conn.close()

print('\n' + '='*80)
print('Search complete')
print('='*80)
