"""
Verify Contact column names match between sit_contact_load.py and sit_generate_contact_docs.py
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

tables = {
    'CO_WORKER': ['CUSTOMER_ID'],
    'CO_PERSON': ['PERSON_ID', 'FIRST_NAME', 'SURNAME', 'DATE_OF_BIRTH'],
    'CO_CUSTOMER': ['CUSTOMER_ID', 'EMAIL_ADDRESS', 'MOBILE_PHONE_NO', 'TELEPHONE1_NO'],
    'CO_EMPLOYMENT_PERIOD': ['WORKER_ID', 'EMPLOYER_ID', 'EFFECTIVE_TO_DATE', 'EFFECTIVE_FROM_DATE']
}

print("\nVerifying Contact Mapping Columns")
print("="*70)

all_correct = True
for table, columns in tables.items():
    print(f"\n{table}:")
    for col in columns:
        cursor.execute(f"""
            SELECT column_name 
            FROM all_tab_columns 
            WHERE table_name = '{table}' 
            AND owner = 'SCH_CO_20'
            AND column_name = '{col}'
        """)
        result = cursor.fetchone()
        if result:
            print(f"  ✅ {col}")
        else:
            print(f"  ❌ {col} - NOT FOUND")
            all_correct = False

cursor.close()
conn.close()

print("\n" + "="*70)
if all_correct:
    print("✅ All contact mapping columns are CORRECT")
else:
    print("❌ Some columns are INCORRECT - need to fix documentation")
