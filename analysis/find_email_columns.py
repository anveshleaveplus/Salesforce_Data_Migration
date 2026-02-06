"""
Find all Oracle tables with EMAIL columns
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

cursor = conn.cursor()
cursor.execute("""
    SELECT table_name, column_name, data_type, data_length, nullable
    FROM all_tab_columns
    WHERE owner = 'SCH_CO_20'
      AND UPPER(column_name) LIKE '%EMAIL%'
    ORDER BY table_name, column_name
""")

print('\n' + '='*80)
print('ORACLE TABLES WITH EMAIL COLUMNS (SCH_CO_20)')
print('='*80)

current_table = None
for row in cursor.fetchall():
    table_name, column_name, data_type, data_length, nullable = row
    
    if table_name != current_table:
        print(f'\n{table_name}:')
        current_table = table_name
    
    null_str = 'NULL' if nullable == 'Y' else 'NOT NULL'
    print(f'  {column_name:30s} {data_type}({data_length}) {null_str}')

# Get sample data from CO_CUSTOMER (most commonly used)
print('\n' + '='*80)
print('SAMPLE EMAIL DATA FROM CO_CUSTOMER')
print('='*80)

cursor.execute("""
    SELECT CUSTOMER_ID, EMAIL_ADDRESS
    FROM SCH_CO_20.CO_CUSTOMER
    WHERE EMAIL_ADDRESS IS NOT NULL
    AND ROWNUM <= 10
    ORDER BY CUSTOMER_ID
""")

for row in cursor.fetchall():
    print(f'  CUSTOMER_ID: {row[0]:10d}  EMAIL: {row[1]}')

cursor.close()
conn.close()
