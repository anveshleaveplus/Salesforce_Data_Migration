"""
Check Oracle EMPLOYER_TYPE_CODE values
"""
import os
from dotenv import load_dotenv
import oracledb

load_dotenv('.env.sit')

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

print('\n' + '='*70)
print('ORACLE - EMPLOYER_TYPE_CODE Values')
print('='*70)

cursor = conn.cursor()

# Get code mapping
cursor.execute("""
    SELECT code_set_id 
    FROM SCH_CO_20.CO_CODE_SET 
    WHERE LOWER(code_set_name) LIKE 'employertypecode%'
""")
result = cursor.fetchone()

if result:
    code_set_id = result[0]
    print(f'\nCode Set ID: {code_set_id}')
    
    # First check what columns exist in CO_CODE
    cursor.execute("""
        SELECT column_name 
        FROM all_tab_columns 
        WHERE table_name = 'CO_CODE' 
        AND owner = 'SCH_CO_20'
        ORDER BY column_id
    """)
    
    print('\nCO_CODE table columns:')
    code_columns = [row[0] for row in cursor.fetchall()]
    for col in code_columns:
        print(f'  - {col}')
    
    # Now query the actual data
    cursor.execute(f"""
        SELECT *
        FROM SCH_CO_20.CO_CODE
        WHERE code_set_id = {code_set_id}
        AND ROWNUM <= 10
    """)
    
    print('\nOracle Code Values (first 10):')
    print('-'*70)
    for row in cursor.fetchall():
        print(f'  {row}')
else:
    print('\n❌ EMPLOYER_TYPE_CODE not found')

# Check actual usage in CO_EMPLOYER
print('\n' + '='*70)
print('Actual Usage in CO_EMPLOYER (Active Employers)')
print('='*70)

cursor.execute("""
    SELECT 
        e.EMPLOYER_TYPE_CODE,
        COUNT(*) as cnt
    FROM SCH_CO_20.CO_EMPLOYER e
    WHERE e.CUSTOMER_ID IN (
        SELECT DISTINCT ep.EMPLOYER_ID
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= 202301
        )
    )
    GROUP BY e.EMPLOYER_TYPE_CODE
    ORDER BY COUNT(*) DESC
""")

results = cursor.fetchall()
total = sum(r[1] for r in results)

print(f'\nTotal Active Employers: {total:,}\n')

for code, count in results:
    pct = (count/total*100) if total > 0 else 0
    code_str = str(code) if code else '(blank/null)'
    print(f'  {code_str:15s} {count:6,} ({pct:5.1f}%)')

cursor.close()
conn.close()

print('\n' + '='*70)
print('MAPPING COMPARISON')
print('='*70)
print('\nSalesforce BusinessEntityType__c picklist values:')
print('  • Sole Trader')
print('  • Partnership')
print('  • Trust')
print('  • Company')
print('\nOracle EMPLOYER_TYPE_CODE should be mapped to these values.')
print('='*70)
