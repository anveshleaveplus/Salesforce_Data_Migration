"""
Check CO_WORKER_TRANSACTION table for OwnersPerformCoveredWork__c mapping
Looking at OWNER_PERFORM_TRADEWORK column
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

print("Checking CO_WORKER_TRANSACTION for OwnersPerformCoveredWork__c")
print("="*80)

cursor = conn.cursor()

# Check if table exists
print("\n1. Verifying CO_WORKER_TRANSACTION table exists:")
cursor.execute("""
    SELECT COUNT(*) 
    FROM ALL_TABLES 
    WHERE TABLE_NAME = 'CO_WORKER_TRANSACTION' 
    AND OWNER = 'SCH_CO_20'
""")
exists = cursor.fetchone()[0]
if exists:
    print("   ✓ Table exists")
else:
    print("   ✗ Table does not exist")
    conn.close()
    exit(1)

# Check columns
print("\n2. Columns in CO_WORKER_TRANSACTION:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_WORKER_TRANSACTION' 
    AND OWNER = 'SCH_CO_20'
    ORDER BY COLUMN_ID
""")
print(f"{'Column Name':<40} {'Type':<15} {'Length':<10} {'Nullable':<10}")
print("-"*80)
for row in cursor:
    print(f"{row[0]:<40} {row[1]:<15} {str(row[2]):<10} {row[3]:<10}")

# Check if OWNER_PERFORM_TRADEWORK column exists
print("\n3. Checking OWNER_PERFORM_TRADEWORK column:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH 
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_WORKER_TRANSACTION' 
    AND OWNER = 'SCH_CO_20'
    AND COLUMN_NAME = 'OWNER_PERFORM_TRADEWORK'
""")
result = cursor.fetchone()
if result:
    print(f"   ✓ Column exists: {result[0]} ({result[1]})")
else:
    print("   ✗ Column OWNER_PERFORM_TRADEWORK not found")
    print("   Searching for similar columns...")
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM ALL_TAB_COLUMNS 
        WHERE TABLE_NAME = 'CO_WORKER_TRANSACTION' 
        AND OWNER = 'SCH_CO_20'
        AND (COLUMN_NAME LIKE '%OWNER%' OR COLUMN_NAME LIKE '%PERFORM%' OR COLUMN_NAME LIKE '%TRADE%')
    """)
    print("   Similar columns found:")
    for row in cursor:
        print(f"      - {row[0]}")

# Check record count
print("\n4. Record count:")
cursor.execute("SELECT COUNT(*) FROM SCH_CO_20.CO_WORKER_TRANSACTION")
total = cursor.fetchone()[0]
print(f"   Total records: {total:,}")

# Check relationship to employers
print("\n5. Checking relationship to employers:")
cursor.execute("""
    SELECT COLUMN_NAME 
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_WORKER_TRANSACTION' 
    AND OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%EMPLOYER%' OR COLUMN_NAME LIKE '%CUSTOMER%')
    ORDER BY COLUMN_NAME
""")
print("   Employer-related columns:")
employer_cols = []
for row in cursor:
    employer_cols.append(row[0])
    print(f"      - {row[0]}")

if not employer_cols:
    print("   (None found - checking for WORKER columns)")
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM ALL_TAB_COLUMNS 
        WHERE TABLE_NAME = 'CO_WORKER_TRANSACTION' 
        AND OWNER = 'SCH_CO_20'
        AND COLUMN_NAME LIKE '%WORKER%'
        ORDER BY COLUMN_NAME
    """)
    print("   Worker-related columns:")
    for row in cursor:
        print(f"      - {row[0]}")

# Check if OWNER_PERFORM_TRADEWORK has data
if result:
    print("\n6. Checking OWNER_PERFORM_TRADEWORK data distribution:")
    cursor.execute("""
        SELECT 
            OWNER_PERFORM_TRADEWORK,
            COUNT(*) as COUNT
        FROM SCH_CO_20.CO_WORKER_TRANSACTION
        GROUP BY OWNER_PERFORM_TRADEWORK
        ORDER BY COUNT DESC
    """)
    print(f"{'Value':<30} {'Count':<15}")
    print("-"*45)
    for row in cursor:
        print(f"{str(row[0]):<30} {row[1]:,}")
    
    # Sample records
    print("\n7. Sample records:")
    cursor.execute("""
        SELECT *
        FROM SCH_CO_20.CO_WORKER_TRANSACTION
        WHERE ROWNUM <= 5
    """)
    columns = [desc[0] for desc in cursor.description]
    print(f"Columns: {', '.join(columns)}")
    print("-"*80)
    for row in cursor:
        print(row)

conn.close()

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
