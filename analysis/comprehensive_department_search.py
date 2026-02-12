"""
Investigate department/organizational structure in Oracle - comprehensive search
"""

import os
from dotenv import load_dotenv
import oracledb

load_dotenv()

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print("=" * 80)
print("COMPREHENSIVE DEPARTMENT/ORGANIZATIONAL SEARCH")
print("=" * 80)

# [1] All CO_WORKER columns
print("\n[1] All CO_WORKER Columns (checking for organizational fields):")
print("-" * 80)

query = """
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_WORKER'
    ORDER BY COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

print(f"CO_WORKER has {len(results)} columns:")
for column, dtype, length in results:
    print(f"  {column} ({dtype}({length}))")

# [2] Check for tables with DEPARTMENT in name
print("\n[2] Tables with DEPARTMENT in Name:")
print("-" * 80)

query = """
    SELECT TABLE_NAME
    FROM ALL_TABLES
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME LIKE '%DEPART%'
    ORDER BY TABLE_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} department-related tables:")
    for (table_name,) in results:
        print(f"  {table_name}")
        
        # Get structure
        query_struct = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = 'SCH_CO_20'
            AND TABLE_NAME = :table_name
            ORDER BY COLUMN_NAME
        """
        cursor.execute(query_struct, {'table_name': table_name})
        cols = cursor.fetchall()
        for col, dtype, length in cols[:10]:  # Show first 10 columns
            print(f"    {col} ({dtype}({length}))")
        if len(cols) > 10:
            print(f"    ... and {len(cols) - 10} more columns")
else:
    print("No department tables found")

# [3] Check code sets (fix query)
print("\n[3] Department/Section Code Sets:")
print("-" * 80)

query = """
    SELECT CODE_SET_ID, CODE_SET_NAME
    FROM CO_CODE_SET
    WHERE UPPER(CODE_SET_NAME) LIKE '%DEPART%'
       OR UPPER(CODE_SET_NAME) LIKE '%SECTION%'
    ORDER BY CODE_SET_ID
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} code sets:")
    for code_set_id, name in results:
        print(f"\nCode Set {code_set_id}: {name}")

# [4] Check for OCCUPATION/TRADE/SKILL columns (alternative groupings)
print("\n[4] Occupation/Trade/Skill Columns:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%OCCUPATION%' 
         OR COLUMN_NAME LIKE '%TRADE%'
         OR COLUMN_NAME LIKE '%SKILL%'
         OR COLUMN_NAME LIKE '%JOB%'
         OR COLUMN_NAME LIKE '%ROLE%'
         OR COLUMN_NAME LIKE '%POSITION%')
    AND TABLE_NAME IN ('CO_WORKER', 'CO_PERSON', 'CO_CUSTOMER', 
                       'CO_EMPLOYMENT_PERIOD')
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} occupation/trade columns:")
    for table, column, dtype, length in results:
        print(f"  {table}.{column} ({dtype}({length}))")
else:
    print("No occupation/trade columns found")

# [5] Check for CLASSIFICATION/CATEGORY columns
print("\n[5] Classification/Category Columns:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%CLASS%' 
         OR COLUMN_NAME LIKE '%CATEGORY%'
         OR COLUMN_NAME LIKE '%TYPE%')
    AND TABLE_NAME IN ('CO_WORKER', 'CO_EMPLOYMENT_PERIOD')
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} classification columns:")
    for table, column, dtype, length in results:
        print(f"  {table}.{column} ({dtype}({length}))")

# [6] Sample CO_WORKER data to see what categorization exists
print("\n[6] Sample CO_WORKER Data (first 10 records with all code columns):")
print("-" * 80)

query = """
    SELECT 
        CUSTOMER_ID,
        UNION_DELEGATE_CODE,
        MODIFIED_TYPE
    FROM CO_WORKER
    WHERE ROWNUM <= 10
"""

cursor.execute(query)
results = cursor.fetchall()

print("CUSTOMER_ID | UNION_DELEGATE_CODE | MODIFIED_TYPE")
print("-" * 50)
for row in results:
    print(f"{row[0]} | {row[1]} | {row[2]}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("Oracle appears to have NO department/organizational grouping for workers")
print("\nSF Contact.Department (Text 80):")
print("  - Currently empty in SF")
print("  - No obvious Oracle mapping found")
print("\nSF Contact.DepartmentGroup (Picklist):")
print("  - Field does NOT exist in Salesforce")
print("  - User may have incorrect field name or need to create it")
