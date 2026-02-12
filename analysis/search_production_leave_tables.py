"""
Search for production leave balance tables (not test/calculation tables)
Look for actual service, balance, and entitlement tracking
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
print("PRODUCTION LEAVE BALANCE TABLES SEARCH")
print("=" * 80)

# [1] Find all tables with customer/worker relationships
print("\n[1] Main Worker/Service Tables:")
print("-" * 80)

query = """
    SELECT TABLE_NAME
    FROM ALL_TABLES
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME IN (
        'CO_SERVICE',
        'CO_WORKER',
        'CO_CUSTOMER',
        'CO_EMPLOYMENT_PERIOD',
        'CO_WORKER_SERVICE',
        'CO_SERVICE_PERIOD'
    )
    ORDER BY TABLE_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} main worker tables:")
    for (table_name,) in results:
        print(f"  {table_name}")

# [2] Check CO_SERVICE table for LSL/leave columns
print("\n[2] CO_SERVICE Table Leave Columns:")
print("-" * 80)

query = """
    SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_SERVICE'
    AND (COLUMN_NAME LIKE '%LSL%' 
         OR COLUMN_NAME LIKE '%LEAVE%'
         OR COLUMN_NAME LIKE '%BALANCE%')
    ORDER BY COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} leave-related columns in CO_SERVICE:")
    for col, dtype, precision, scale in results:
        type_str = f"{dtype}({precision},{scale})" if precision else dtype
        print(f"  {col} ({type_str})")
else:
    print("No leave columns in CO_SERVICE")

# Sample CO_SERVICE data
try:
    query = """
        SELECT 
            WORKER,
            EMPLOYER,
            SERVICE_WEEKS,
            PERIOD_START,
            PERIOD_END
        FROM CO_SERVICE
        WHERE ROWNUM <= 5
    """
    cursor.execute(query)
    samples = cursor.fetchall()
    
    if samples:
        print("\n  Sample CO_SERVICE records:")
        print(f"  {'WORKER':<12} {'EMPLOYER':<12} {'WEEKS':<8} {'START':<12} {'END':<12}")
        print(f"  {'-'*58}")
        for row in samples:
            print(f"  {str(row[0]):<12} {str(row[1]):<12} {str(row[2]):<8} {str(row[3]):<12} {str(row[4]):<12}")
except Exception as e:
    print(f"  Sample query error: {e}")

# [3] Check for account/balance tables
print("\n[3] Account/Balance Tables:")
print("-" * 80)

query = """
    SELECT TABLE_NAME
    FROM ALL_TABLES
    WHERE OWNER = 'SCH_CO_20'
    AND (TABLE_NAME LIKE '%ACCOUNT%' 
         OR TABLE_NAME LIKE '%BALANCE%'
         OR TABLE_NAME LIKE 'CO_WORKER_%'
         OR TABLE_NAME LIKE 'CO_WSC%')
    AND TABLE_NAME NOT LIKE '%_V%'
    AND TABLE_NAME NOT LIKE '%TEST%'
    AND TABLE_NAME NOT LIKE '%TMP%'
    AND TABLE_NAME NOT LIKE '%_JN'
    ORDER BY TABLE_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} account/balance tables:")
    for (table_name,) in results[:20]:
        print(f"  {table_name}")
    if len(results) > 20:
        print(f"  ... and {len(results) - 20} more")

# [4] Check CO_WORKER table columns
print("\n[4] All CO_WORKER Columns:")
print("-" * 80)

query = """
    SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_WORKER'
    ORDER BY COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

print(f"CO_WORKER has {len(results)} columns:")
for col, dtype, precision, scale in results:
    type_str = f"{dtype}({precision},{scale})" if precision else f"{dtype}"
    print(f"  {col} ({type_str})")

# [5] Check for actual balance tracking table
print("\n[5] Tables with 'CURRENT' or 'BALANCE' in Name:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, NUM_ROWS
    FROM ALL_TABLES
    WHERE OWNER = 'SCH_CO_20'
    AND (TABLE_NAME LIKE '%CURRENT%' 
         OR TABLE_NAME LIKE '%BALANCE%')
    AND TABLE_NAME NOT LIKE '%_V%'
    AND TABLE_NAME NOT LIKE '%TEST%'
    AND TABLE_NAME NOT LIKE '%TMP%'
    ORDER BY TABLE_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} current/balance tables:")
    for table_name, num_rows in results:
        rows_str = f"({num_rows:,} rows)" if num_rows else "(rows unknown)"
        print(f"  {table_name} {rows_str}")
        
        # Get structure of interesting tables
        if 'BALANCE' in table_name or 'CURRENT' in table_name:
            query_cols = """
                SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = 'SCH_CO_20'
                AND TABLE_NAME = :table_name
                ORDER BY COLUMN_NAME
            """
            cursor.execute(query_cols, {'table_name': table_name})
            cols = cursor.fetchall()
            
            if cols:
                print(f"    Columns ({len(cols)}):")
                for col, dtype, precision, scale in cols[:15]:
                    type_str = f"{dtype}({precision},{scale})" if precision else dtype
                    print(f"      {col} ({type_str})")
                if len(cols) > 15:
                    print(f"      ... and {len(cols) - 15} more columns")
else:
    print("No current/balance tables found")

# [6] Check ACTUARIAL tables (might have leave calculations)
print("\n[6] CO_ACTUARIAL Table Investigation:")
print("-" * 80)

try:
    query = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = 'SCH_CO_20'
        AND TABLE_NAME = 'CO_ACTUARIAL'
        AND (COLUMN_NAME LIKE '%CUSTOMER%'
             OR COLUMN_NAME LIKE '%WORKER%'
             OR COLUMN_NAME LIKE '%LSL%'
             OR COLUMN_NAME LIKE '%LEAVE%')
        ORDER BY COLUMN_NAME
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    if results:
        print(f"Found {len(results)} relevant columns in CO_ACTUARIAL:")
        for col, dtype, precision, scale in results:
            type_str = f"{dtype}({precision},{scale})" if precision else dtype
            print(f"  {col} ({type_str})")
        
        # Check if it's a view or table with data
        query_count = "SELECT COUNT(*) FROM CO_ACTUARIAL WHERE ROWNUM = 1"
        cursor.execute(query_count)
        count = cursor.fetchone()
        if count and count[0] > 0:
            print(f"\n  CO_ACTUARIAL has data (queryable)")
        else:
            print(f"\n  CO_ACTUARIAL appears empty or is a view")
    else:
        print("No relevant columns in CO_ACTUARIAL")
except Exception as e:
    print(f"CO_ACTUARIAL not accessible: {e}")

# [7] Check for summary/aggregate tables
print("\n[7] Summary/Aggregate Tables with Leave Data:")
print("-" * 80)

query = """
    SELECT TABLE_NAME
    FROM ALL_TABLES
    WHERE OWNER = 'SCH_CO_20'
    AND (TABLE_NAME LIKE '%TOTAL%' 
         OR TABLE_NAME LIKE '%SUMMARY%'
         OR TABLE_NAME LIKE '%AGGREGATE%')
    AND TABLE_NAME NOT LIKE '%_V%'
    AND TABLE_NAME NOT LIKE '%TEST%'
    AND TABLE_NAME NOT LIKE '%TMP%'
    ORDER BY TABLE_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} summary tables:")
    for (table_name,) in results:
        print(f"  {table_name}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print("\nNeed to identify production table with current leave balance")
print("Check if CO_WORKER has LSL balance fields directly")
print("Or if there's a separate balance tracking table linked to CUSTOMER_ID")
