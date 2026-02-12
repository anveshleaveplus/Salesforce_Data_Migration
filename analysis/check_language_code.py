"""
Check LANGUAGE_CODE in CO_PERSON table
"""
import oracledb
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv('.env.sit')

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

print("Checking LANGUAGE_CODE in CO_PERSON")
print("="*80)

# Check if column exists and get sample data
query = """
SELECT 
    p.PERSON_ID,
    p.FIRST_NAME,
    p.SURNAME,
    p.LANGUAGE_CODE
FROM SCH_CO_20.CO_PERSON p
WHERE ROWNUM <= 100
"""

try:
    df = pd.read_sql(query, conn)
    print(f"\n✓ Column exists - Retrieved {len(df)} rows\n")
    
    # Check population
    total = len(df)
    populated = df['LANGUAGE_CODE'].notna().sum()
    print(f"Population: {populated}/{total} ({populated/total*100:.2f}%)")
    
    # Check unique values
    print(f"\nUnique LANGUAGE_CODE values:")
    value_counts = df['LANGUAGE_CODE'].value_counts()
    for value, count in value_counts.items():
        print(f"  {value}: {count} ({count/total*100:.1f}%)")
    
    null_count = df['LANGUAGE_CODE'].isna().sum()
    if null_count > 0:
        print(f"  NULL: {null_count} ({null_count/total*100:.1f}%)")
    
    # Show samples
    print(f"\nSample records:")
    print(df[['FIRST_NAME', 'SURNAME', 'LANGUAGE_CODE']].head(10).to_string(index=False))
    
    # Check if there's a code mapping
    print("\n" + "="*80)
    print("Checking for LANGUAGE_CODE code set...")
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT code_set_id 
        FROM SCH_CO_20.CO_CODE_SET 
        WHERE LOWER(code_set_name) LIKE '%language%'
    """)
    
    code_sets = cursor.fetchall()
    if code_sets:
        print(f"Found {len(code_sets)} language-related code set(s):")
        for cs in code_sets:
            code_set_id = cs[0]
            cursor.execute(f"""
                SELECT value, description 
                FROM SCH_CO_20.CO_CODE 
                WHERE code_set_id = {code_set_id}
                ORDER BY value
            """)
            print(f"\n  Code Set ID: {code_set_id}")
            for row in cursor:
                print(f"    {row[0]} → {row[1]}")
    else:
        print("No language code set found")
    
except Exception as e:
    print(f"✗ Error: {e}")

conn.close()
print("\n" + "="*80)
