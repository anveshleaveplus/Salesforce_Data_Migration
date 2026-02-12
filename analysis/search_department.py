"""
Search Oracle for Department and DepartmentGroup mappings
SF Fields:
- Contact.Department (Text 80)
- Contact.DepartmentGroup (Picklist)
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
print("DEPARTMENT AND DEPARTMENTGROUP FIELD SEARCH")
print("=" * 80)

# [1] Search for DEPARTMENT columns
print("\n[1] DEPARTMENT Columns in Worker/Person/Customer Tables:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND COLUMN_NAME LIKE '%DEPART%'
    AND TABLE_NAME IN ('CO_WORKER', 'CO_PERSON', 'CO_CUSTOMER', 
                       'CO_EMPLOYMENT_PERIOD', 'CO_EMPLOYER')
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} department columns:")
    for table, column, dtype, length in results:
        print(f"  {table}.{column} ({dtype}({length}))")
else:
    print("No DEPARTMENT columns found")

# [2] Search for DIVISION/SECTION columns (alternatives)
print("\n[2] DIVISION/SECTION/UNIT Columns:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%DIVISION%' 
         OR COLUMN_NAME LIKE '%SECTION%'
         OR COLUMN_NAME LIKE '%UNIT%')
    AND TABLE_NAME IN ('CO_WORKER', 'CO_PERSON', 'CO_CUSTOMER', 
                       'CO_EMPLOYMENT_PERIOD', 'CO_EMPLOYER')
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} division/section columns:")
    for table, column, dtype, length in results:
        print(f"  {table}.{column} ({dtype}({length}))")
else:
    print("No DIVISION/SECTION columns found")

# [3] Check code sets for Department/Division
print("\n[3] Code Sets with DEPART/DIVISION Keywords:")
print("-" * 80)

query = """
    SELECT CODE_SET_ID, CODE_SET_NAME
    FROM CO_CODE_SET
    WHERE UPPER(CODE_SET_NAME) LIKE '%DEPART%'
       OR UPPER(CODE_SET_NAME) LIKE '%DIVISION%'
       OR UPPER(CODE_SET_NAME) LIKE '%SECTION%'
       OR UPPER(CODE_SET_NAME) LIKE '%UNIT%'
    ORDER BY CODE_SET_ID
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} relevant code sets:")
    for code_set_id, name in results:
        print(f"  Code Set {code_set_id}: {name}")
        
        # Get codes for each set
        query_codes = """
            SELECT CODE, DESCRIPTION
            FROM CO_CODE
            WHERE CODE_SET_ID = :code_set_id
            ORDER BY CODE
        """
        cursor.execute(query_codes, {'code_set_id': code_set_id})
        codes = cursor.fetchall()
        
        if codes and len(codes) <= 20:
            for code, desc in codes:
                print(f"    {code}: {desc}")
        elif codes:
            print(f"    ({len(codes)} values - showing first 10)")
            for code, desc in codes[:10]:
                print(f"    {code}: {desc}")
else:
    print("No department/division code sets found")

# [4] Check CO_WORKER columns for organizational info
print("\n[4] CO_WORKER Organizational Columns:")
print("-" * 80)

query = """
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_WORKER'
    AND (COLUMN_NAME LIKE '%CODE%' 
         OR COLUMN_NAME LIKE '%TYPE%'
         OR COLUMN_NAME LIKE '%CATEGORY%'
         OR COLUMN_NAME LIKE '%CLASS%'
         OR COLUMN_NAME LIKE '%GROUP%')
    ORDER BY COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} organizational columns in CO_WORKER:")
    for column, dtype, length in results:
        print(f"  {column} ({dtype}({length}))")

# [5] Check CO_EMPLOYER for department info
print("\n[5] CO_EMPLOYER Department/Division Columns:")
print("-" * 80)

query = """
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_EMPLOYER'
    AND (COLUMN_NAME LIKE '%DEPART%' 
         OR COLUMN_NAME LIKE '%DIVISION%'
         OR COLUMN_NAME LIKE '%SECTION%'
         OR COLUMN_NAME LIKE '%UNIT%')
    ORDER BY COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} columns in CO_EMPLOYER:")
    for column, dtype, length in results:
        print(f"  {column} ({dtype}({length}))")
else:
    print("No department columns in CO_EMPLOYER")

# [6] Sample data if department columns exist
print("\n[6] Sample Data from Active Workers:")
print("-" * 80)

# Check if we have any department-related data
query = """
    SELECT COLUMN_NAME
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_WORKER'
    AND (COLUMN_NAME LIKE '%DEPART%' 
         OR COLUMN_NAME LIKE '%DIVISION%'
         OR COLUMN_NAME LIKE '%SECTION%')
"""

cursor.execute(query)
dept_columns = [row[0] for row in cursor.fetchall()]

if dept_columns:
    # Build dynamic query with available columns
    cols_str = ', '.join([f'w.{col}' for col in dept_columns])
    
    query = f"""
        SELECT w.CUSTOMER_ID, {cols_str}
        FROM SCH_CO_20.CO_WORKER w
        WHERE ROWNUM <= 20
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    print(f"Sample worker records with department columns:")
    print(f"Columns: CUSTOMER_ID, {', '.join(dept_columns)}")
    print("-" * 80)
    for row in results:
        print(f"  {row}")
else:
    print("No department columns found in CO_WORKER")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("SEARCH COMPLETE")
print("=" * 80)
print("\nSF Fields:")
print("  Contact.Department (Text 80) - Free text field")
print("  Contact.DepartmentGroup (Picklist) - Predefined values")
print("\nNext: If found, check SF DepartmentGroup picklist values to map Oracle codes")
