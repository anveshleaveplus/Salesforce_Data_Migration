"""
Check Oracle CO_CODE for union codes (code_set_id = 316)
"""
import oracledb
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

cursor = conn.cursor()

print("="*80)
print("ORACLE UNION CODE ANALYSIS - Code Set 316")
print("="*80)

# 1. Check code set info
print("\n[1] Code Set 316 Information:")
print("-"*80)

codeset_query = """
SELECT code_set_id, code_set_name
FROM SCH_CO_20.CO_CODE_SET
WHERE code_set_id = 316
"""

cursor.execute(codeset_query)
codeset = cursor.fetchone()

if codeset:
    print(f"Code Set ID:   {codeset[0]}")
    print(f"Name:          {codeset[1]}")
else:
    print("Code set 316 not found!")

# 2. Get all union codes from CO_CODE
print("\n[2] Union Codes in CO_CODE (code_set_id = 316):")
print("-"*80)

codes_query = """
SELECT 
    code_set_id,
    VALUE as code_value,
    DESCRIPTION as code_description
FROM SCH_CO_20.CO_CODE
WHERE code_set_id = 316
ORDER BY VALUE
"""

cursor.execute(codes_query)
union_codes = cursor.fetchall()

print(f"{'VALUE':<15} {'DESCRIPTION':<50}")
print("-"*80)

picklist_values = []
for row in union_codes:
    value = row[1] if row[1] else ''
    desc = row[2] if row[2] else ''
    print(f"{str(value):<15} {str(desc):<50}")
    picklist_values.append((value, desc))

print(f"\nTotal union codes: {len(union_codes)}")

# 3. Check which field in CO_WORKER stores the union code
print("\n[3] Searching for Union Code field in CO_WORKER:")
print("-"*80)

# Check all columns with 'union' in name
worker_cols_query = """
SELECT column_name, data_type, data_length
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND table_name = 'CO_WORKER'
    AND LOWER(column_name) LIKE '%union%'
ORDER BY column_name
"""

cursor.execute(worker_cols_query)
worker_cols = cursor.fetchall()

if worker_cols:
    print(f"{'COLUMN_NAME':<30} {'DATA_TYPE':<15} {'LENGTH':<10}")
    print("-"*80)
    for col in worker_cols:
        print(f"{col[0]:<30} {col[1]:<15} {str(col[2]):<10}")
else:
    print("No columns with 'union' found in CO_WORKER")

# 4. Sample data - check if UNION_DELEGATE_CODE contains union codes
print("\n[4] Sample Union Codes in CO_WORKER:")
print("-"*80)

sample_query = """
SELECT 
    w.CUSTOMER_ID,
    w.UNION_DELEGATE_CODE,
    c.VALUE as union_code_value,
    c.DESCRIPTION as union_description
FROM SCH_CO_20.CO_WORKER w
LEFT JOIN SCH_CO_20.CO_CODE c 
    ON w.UNION_DELEGATE_CODE = c.VALUE 
    AND c.code_set_id = 316
WHERE w.UNION_DELEGATE_CODE IS NOT NULL 
    AND w.UNION_DELEGATE_CODE != '0'
    AND ROWNUM <= 20
ORDER BY w.UNION_DELEGATE_CODE
"""

cursor.execute(sample_query)
samples = cursor.fetchall()

print(f"{'CUSTOMER_ID':<15} {'UNION_CODE':<15} {'CODE_VALUE':<15} {'DESCRIPTION':<30}")
print("-"*80)

for row in samples:
    cid = row[0]
    code = row[1] if row[1] else 'NULL'
    value = row[2] if row[2] else 'NO MATCH'
    desc = row[3] if row[3] else ''
    print(f"{cid:<15} {str(code):<15} {str(value):<15} {str(desc):<30}")

# 5. Check coverage
print("\n[5] Union Code Coverage:")
print("-"*80)

coverage_query = """
SELECT 
    w.UNION_DELEGATE_CODE,
    COUNT(*) as count,
    MAX(c.DESCRIPTION) as description
FROM SCH_CO_20.CO_WORKER w
LEFT JOIN SCH_CO_20.CO_CODE c 
    ON w.UNION_DELEGATE_CODE = c.VALUE 
    AND c.code_set_id = 316
WHERE w.UNION_DELEGATE_CODE IS NOT NULL 
    AND w.UNION_DELEGATE_CODE != '0'
GROUP BY w.UNION_DELEGATE_CODE
ORDER BY COUNT(*) DESC
"""

cursor.execute(coverage_query)
coverage = cursor.fetchall()

print(f"{'UNION_CODE':<15} {'COUNT':<12} {'DESCRIPTION':<40}")
print("-"*80)
total_union_members = 0
for row in coverage:
    code = row[0] if row[0] else ''
    count = row[1]
    desc = row[2] if row[2] else 'Not in CO_CODE set 316'
    print(f"{str(code):<15} {count:<12,} {str(desc):<40}")
    total_union_members += count

print("-"*80)
print(f"{'TOTAL':<15} {total_union_members:<12,}")

print("\n" + "="*80)
print("SALESFORCE FIELD CONFIGURATION NEEDED:")
print("="*80)
print("Field: Contact.Union__c")
print("Current Type: Lookup(Account) ❌")
print("Needed Type: Picklist ✓")
print()
print("Picklist Values (from CO_CODE set 316):")
for value, desc in picklist_values:
    print(f"  • {value} - {desc}")
print("="*80)

cursor.close()
conn.close()
