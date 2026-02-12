"""
Search Oracle for field officer related columns
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
print("ORACLE FIELD OFFICER SEARCH")
print("="*80)

# 1. Search for columns with 'FIELD' in name
print("\n[1] Columns containing 'FIELD' in SCH_CO_20 tables:")
print("-"*80)

field_query = """
SELECT table_name, column_name, data_type, data_length
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND LOWER(column_name) LIKE '%field%'
ORDER BY table_name, column_name
"""

cursor.execute(field_query)
field_cols = cursor.fetchall()

if field_cols:
    print(f"{'TABLE_NAME':<30} {'COLUMN_NAME':<35} {'TYPE':<15} {'LENGTH':<10}")
    print("-"*80)
    for row in field_cols:
        print(f"{row[0]:<30} {row[1]:<35} {row[2]:<15} {str(row[3]):<10}")
    print(f"\nTotal columns with 'field': {len(field_cols)}")
else:
    print("No columns with 'field' found")

# 2. Search for columns with 'OFFICER' in name
print("\n[2] Columns containing 'OFFICER' in SCH_CO_20 tables:")
print("-"*80)

officer_query = """
SELECT table_name, column_name, data_type, data_length
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND LOWER(column_name) LIKE '%officer%'
ORDER BY table_name, column_name
"""

cursor.execute(officer_query)
officer_cols = cursor.fetchall()

if officer_cols:
    print(f"{'TABLE_NAME':<30} {'COLUMN_NAME':<35} {'TYPE':<15} {'LENGTH':<10}")
    print("-"*80)
    for row in officer_cols:
        print(f"{row[0]:<30} {row[1]:<35} {row[2]:<15} {str(row[3]):<10}")
    print(f"\nTotal columns with 'officer': {len(officer_cols)}")
else:
    print("No columns with 'officer' found")

# 3. Search for columns with 'ALLOCAT' in name
print("\n[3] Columns containing 'ALLOCAT' in SCH_CO_20 tables:")
print("-"*80)

allocat_query = """
SELECT table_name, column_name, data_type, data_length
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND LOWER(column_name) LIKE '%allocat%'
ORDER BY table_name, column_name
"""

cursor.execute(allocat_query)
allocat_cols = cursor.fetchall()

if allocat_cols:
    print(f"{'TABLE_NAME':<30} {'COLUMN_NAME':<35} {'TYPE':<15} {'LENGTH':<10}")
    print("-"*80)
    for row in allocat_cols:
        print(f"{row[0]:<30} {row[1]:<35} {row[2]:<15} {str(row[3]):<10}")
    print(f"\nTotal columns with 'allocat': {len(allocat_cols)}")
else:
    print("No columns with 'allocat' found")

# 4. Search for columns with 'ASSIGN' in name
print("\n[4] Columns containing 'ASSIGN' in SCH_CO_20 tables:")
print("-"*80)

assign_query = """
SELECT table_name, column_name, data_type, data_length
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND LOWER(column_name) LIKE '%assign%'
ORDER BY table_name, column_name
"""

cursor.execute(assign_query)
assign_cols = cursor.fetchall()

if assign_cols:
    print(f"{'TABLE_NAME':<30} {'COLUMN_NAME':<35} {'TYPE':<15} {'LENGTH':<10}")
    print("-"*80)
    for row in assign_cols:
        print(f"{row[0]:<30} {row[1]:<35} {row[2]:<15} {str(row[3]):<10}")
    print(f"\nTotal columns with 'assign': {len(assign_cols)}")
else:
    print("No columns with 'assign' found")

# 5. Check CO_WORKER table for any relevant columns
print("\n[5] All columns in CO_WORKER table:")
print("-"*80)

worker_query = """
SELECT column_name, data_type, data_length, nullable
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND table_name = 'CO_WORKER'
ORDER BY column_name
"""

cursor.execute(worker_query)
worker_cols = cursor.fetchall()

print(f"{'COLUMN_NAME':<35} {'TYPE':<15} {'LENGTH':<10} {'NULLABLE':<10}")
print("-"*80)
for row in worker_cols:
    print(f"{row[0]:<35} {row[1]:<15} {str(row[2]):<10} {row[3]:<10}")

print(f"\nTotal columns in CO_WORKER: {len(worker_cols)}")

# 6. Check if there's a code set related to officers
print("\n[6] Code sets containing 'officer' or 'field':")
print("-"*80)

codeset_query = """
SELECT code_set_id, code_set_name
FROM SCH_CO_20.CO_CODE_SET
WHERE LOWER(code_set_name) LIKE '%officer%'
   OR LOWER(code_set_name) LIKE '%field%'
ORDER BY code_set_id
"""

cursor.execute(codeset_query)
codesets = cursor.fetchall()

if codesets:
    print(f"{'CODE_SET_ID':<15} {'CODE_SET_NAME':<40}")
    print("-"*80)
    for row in codesets:
        print(f"{row[0]:<15} {row[1]:<40}")
else:
    print("No code sets with 'officer' or 'field' found")

print("\n" + "="*80)
print("SALESFORCE FIELD:")
print("-"*80)
print("Field: Contact.FieldOfficerAllocated__c")
print("Type: Unknown (need to check Salesforce)")
print()
print("❓ No obvious Oracle column found for field officer allocation")
print("📌 Need to determine:")
print("   1. What does FieldOfficerAllocated__c represent?")
print("   2. Is it a boolean (allocated Y/N)?")
print("   3. Is it a lookup to user/employee?")
print("   4. Is there a related table we haven't checked?")
print("="*80)

cursor.close()
conn.close()
