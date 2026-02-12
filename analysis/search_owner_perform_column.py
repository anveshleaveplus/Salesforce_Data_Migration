"""
Search for OWNER_PERFORM_TRADEWORK or similar columns across all CO tables
"""
import oracledb
from dotenv import load_dotenv
import os

load_dotenv('.env.sit')

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

print("Searching for OWNER_PERFORM_TRADEWORK across all tables")
print("="*80)

cursor = conn.cursor()

# Search for exact column name
print("\n1. Searching for OWNER_PERFORM_TRADEWORK column:")
cursor.execute("""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND COLUMN_NAME = 'OWNER_PERFORM_TRADEWORK'
""")
result = cursor.fetchone()
if result:
    print(f"   Found in: {result[0]}.{result[1]} ({result[2]})")
else:
    print("   Not found")

# Search for similar column names
print("\n2. Searching for columns with 'OWNER' and 'PERFORM':")
cursor.execute("""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%OWNER%' AND COLUMN_NAME LIKE '%PERFORM%')
    ORDER BY TABLE_NAME, COLUMN_NAME
""")
found = False
for row in cursor:
    found = True
    print(f"   {row[0]}.{row[1]} ({row[2]})")
if not found:
    print("   (None found)")

# Search for columns with 'TRADE' and 'WORK'
print("\n3. Searching for columns with 'TRADE' and 'WORK':")
cursor.execute("""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND COLUMN_NAME LIKE '%TRADE%'
    AND COLUMN_NAME LIKE '%WORK%'
    ORDER BY TABLE_NAME, COLUMN_NAME
""")
found = False
for row in cursor:
    found = True
    print(f"   {row[0]}.{row[1]} ({row[2]})")
if not found:
    print("   (None found)")

# Search for any columns with 'OWNER_PERFORM'
print("\n4. Searching for columns with 'OWNER_PERFORM':")
cursor.execute("""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND COLUMN_NAME LIKE '%OWNER%PERFORM%'
    ORDER BY TABLE_NAME, COLUMN_NAME
""")
found = False
for row in cursor:
    found = True
    print(f"   {row[0]}.{row[1]} ({row[2]})")
if not found:
    print("   (None found)")

# Check CO_EMPLOYER table for owner-related fields
print("\n5. Checking CO_EMPLOYER for owner/perform/covered work fields:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_EMPLOYER'
    AND (COLUMN_NAME LIKE '%OWNER%' 
         OR COLUMN_NAME LIKE '%PERFORM%' 
         OR COLUMN_NAME LIKE '%COVERED%'
         OR COLUMN_NAME LIKE '%TRADE%')
    ORDER BY COLUMN_NAME
""")
found = False
for row in cursor:
    found = True
    print(f"   CO_EMPLOYER.{row[0]} ({row[1]})")
if not found:
    print("   (None found)")

# Check all CO_EMPLOYER columns
print("\n6. All columns in CO_EMPLOYER (showing potential candidates):")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_EMPLOYER'
    AND DATA_TYPE IN ('CHAR', 'VARCHAR2', 'NUMBER')
    ORDER BY COLUMN_NAME
""")
print("   (Showing first 30 columns)")
count = 0
for row in cursor:
    count += 1
    if count <= 30:
        print(f"   {row[0]} ({row[1]})")

# Check CO_WORKER for owner indicators
print("\n7. Checking CO_WORKER for owner/proprietor indicators:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_WORKER'
    AND (COLUMN_NAME LIKE '%OWNER%' 
         OR COLUMN_NAME LIKE '%PROPRIETOR%'
         OR COLUMN_NAME LIKE '%DIRECTOR%'
         OR COLUMN_NAME LIKE '%PARTNER%')
    ORDER BY COLUMN_NAME
""")
found = False
for row in cursor:
    found = True
    print(f"   CO_WORKER.{row[0]} ({row[1]})")
if not found:
    print("   (None found)")

conn.close()

print("\n" + "="*80)
print("CONCLUSION:")
print("OWNER_PERFORM_TRADEWORK column does not exist in the Oracle schema")
print("This mapping may need clarification or the column name may be incorrect")
print("="*80)
