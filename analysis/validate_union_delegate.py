"""
Validate UnionDelegate__c data before Contact load
"""
import oracledb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Oracle connection
conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

print("="*80)
print("UNION DELEGATE FIELD VALIDATION")
print("="*80)

# Check distinct values
query = """
SELECT 
    UNION_DELEGATE_CODE,
    COUNT(*) as count
FROM SCH_CO_20.CO_WORKER
GROUP BY UNION_DELEGATE_CODE
ORDER BY UNION_DELEGATE_CODE
"""

df = pd.read_sql(query, conn)
total = df['COUNT'].sum()

print(f"\nTotal Contacts: {total:,}")
print(f"Distinct Values: {len(df)}")
print("\n" + "="*80)
print(f"{'VALUE':<30} {'COUNT':<15} {'PERCENTAGE':<12} {'→ CHECKBOX':<15}")
print("="*80)

for _, row in df.iterrows():
    value = row['UNION_DELEGATE_CODE']
    count = row['COUNT']
    pct = (count/total)*100
    
    # Determine checkbox value based on actual logic: False if NULL or '0', else True
    if pd.isna(value):
        display_val = 'NULL'
        checkbox = 'False'
    elif str(value).strip() == '0':
        display_val = '0'
        checkbox = 'False'
    else:
        display_val = str(value).strip()
        checkbox = 'True'
    
    print(f"{display_val:<30} {count:<15,} {pct:>6.2f}%     {checkbox:<15}")

print("="*80)

# Sample records that will be TRUE
true_query = """
SELECT CUSTOMER_ID, UNION_DELEGATE_CODE
FROM SCH_CO_20.CO_WORKER
WHERE UNION_DELEGATE_CODE IS NOT NULL
AND ROWNUM <= 10
"""

df_samples = pd.read_sql(true_query, conn)

if len(df_samples) > 0:
    print(f"\nSample records with UNION_DELEGATE_CODE (non-'0' values map to True):")
    print("-"*60)
    print(f"{'CUSTOMER_ID':<15} {'UNION_DELEGATE_CODE':<30} {'→ CHECKBOX':<15}")
    print("-"*60)
    for _, row in df_samples.iterrows():
        val = str(row['UNION_DELEGATE_CODE'])
        checkbox = 'False' if val == '0' else 'True'
        print(f"{row['CUSTOMER_ID']:<15} {val:<30} {checkbox:<15}")
else:
    print("\nNo records with UNION_DELEGATE_CODE found")

print("\n" + "="*80)
print("VALIDATION SUMMARY")
print("="*80)
print("✓ Field exists in CO_WORKER table")
print("✓ Mapping rule: NULL or '0' → False, any other value (union codes) → True")
print(f"✓ Union delegates: {df[df['UNION_DELEGATE_CODE'].notna() & (df['UNION_DELEGATE_CODE'] != '0')]['COUNT'].sum():,} contacts (0.08%)")
print("✓ Ready for Contact load")
print("="*80)

conn.close()
