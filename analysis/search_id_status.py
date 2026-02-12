"""
Search Oracle for ID Status related columns for Contact.IDStatus__c picklist mapping
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
print("ORACLE ID STATUS SEARCH - Contact.IDStatus__c (Picklist)")
print("="*80)

# 1. Search columns with 'ID' and 'STATUS' in CO_WORKER, CO_PERSON, CO_CUSTOMER
print("\n[1] Columns with 'ID_STATUS' or 'IDSTATUS' in name:")
print("-"*80)

id_status_query = """
SELECT table_name, column_name, data_type, data_length
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND table_name IN ('CO_WORKER', 'CO_PERSON', 'CO_CUSTOMER')
    AND (LOWER(column_name) LIKE '%id_status%' OR LOWER(column_name) LIKE '%idstatus%')
ORDER BY table_name, column_name
"""

cursor.execute(id_status_query)
id_status_cols = cursor.fetchall()

if id_status_cols:
    print(f"{'TABLE':<20} {'COLUMN':<35} {'TYPE':<15} {'LENGTH':<10}")
    print("-"*80)
    for row in id_status_cols:
        print(f"{row[0]:<20} {row[1]:<35} {row[2]:<15} {str(row[3]):<10}")
else:
    print("No columns with 'id_status' found")

# 2. Search for 'IDENTIFICATION' columns
print("\n[2] Columns with 'IDENTIFICATION' in name:")
print("-"*80)

ident_query = """
SELECT table_name, column_name, data_type, data_length
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND table_name IN ('CO_WORKER', 'CO_PERSON', 'CO_CUSTOMER')
    AND LOWER(column_name) LIKE '%identifi%'
ORDER BY table_name, column_name
"""

cursor.execute(ident_query)
ident_cols = cursor.fetchall()

if ident_cols:
    print(f"{'TABLE':<20} {'COLUMN':<35} {'TYPE':<15} {'LENGTH':<10}")
    print("-"*80)
    for row in ident_cols:
        print(f"{row[0]:<20} {row[1]:<35} {row[2]:<15} {str(row[3]):<10}")
else:
    print("No columns with 'identification' found")

# 3. Search code sets for ID or identification related
print("\n[3] Code Sets with 'ID' or 'IDENTIF' in name:")
print("-"*80)

codeset_query = """
SELECT code_set_id, code_set_name
FROM SCH_CO_20.CO_CODE_SET
WHERE LOWER(code_set_name) LIKE '%id%'
   OR LOWER(code_set_name) LIKE '%identif%'
ORDER BY code_set_id
"""

cursor.execute(codeset_query)
codesets = cursor.fetchall()

if codesets:
    print(f"{'CODE_SET_ID':<15} {'CODE_SET_NAME':<50}")
    print("-"*80)
    for row in codesets:
        print(f"{row[0]:<15} {row[1]:<50}")
    
    # Show values for promising code sets
    if codesets:
        print("\n[4] Sample values from relevant code sets:")
        print("-"*80)
        
        for cs_id, cs_name in codesets[:5]:  # Check first 5
            print(f"\nCode Set {cs_id}: {cs_name}")
            print("-"*60)
            
            values_query = f"""
            SELECT VALUE, DESCRIPTION
            FROM SCH_CO_20.CO_CODE
            WHERE code_set_id = {cs_id}
            ORDER BY VALUE
            """
            
            cursor.execute(values_query)
            values = cursor.fetchall()
            
            if values:
                print(f"{'VALUE':<20} {'DESCRIPTION':<40}")
                print("-"*60)
                for v, d in values[:10]:  # Show first 10
                    print(f"{str(v):<20} {str(d) if d else 'NULL':<40}")
                if len(values) > 10:
                    print(f"... and {len(values)-10} more values")
else:
    print("No code sets with 'id' or 'identification' found")

# 5. Check CO_PERSON for any status-related columns
print("\n[5] All columns in CO_PERSON with 'STATUS' or 'CODE':")
print("-"*80)

person_query = """
SELECT column_name, data_type, data_length
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND table_name = 'CO_PERSON'
    AND (LOWER(column_name) LIKE '%status%' OR LOWER(column_name) LIKE '%code%')
ORDER BY column_name
"""

cursor.execute(person_query)
person_cols = cursor.fetchall()

print(f"{'COLUMN':<35} {'TYPE':<15} {'LENGTH':<10}")
print("-"*80)
for row in person_cols:
    print(f"{row[0]:<35} {row[1]:<15} {str(row[2]):<10}")

print("\n" + "="*80)
print("❓ Need more context:")
print("  - What does IDStatus__c represent in Salesforce?")
print("  - ID verification status? Document status? Registration status?")
print("  - What are the picklist values?")
print("="*80)

cursor.close()
conn.close()
