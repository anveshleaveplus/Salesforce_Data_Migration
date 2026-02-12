"""
Search Oracle for CurrentLeaveBalance__c mapping
SF Field: Contact.CurrentLeaveBalance__c (Number 18,0)
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
print("CURRENT LEAVE BALANCE SEARCH")
print("=" * 80)
print("SF Field: Contact.CurrentLeaveBalance__c (Number 18,0)")

# [1] Search for LEAVE columns
print("\n[1] LEAVE Columns in Oracle:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND COLUMN_NAME LIKE '%LEAVE%'
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} LEAVE columns:")
    for table, column, dtype, length, precision, scale in results:
        type_str = f"{dtype}({precision},{scale})" if precision else f"{dtype}({length})"
        print(f"  {table}.{column} ({type_str})")
else:
    print("No LEAVE columns found")

# [2] Search for BALANCE columns
print("\n[2] BALANCE Columns in Oracle:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND COLUMN_NAME LIKE '%BALANCE%'
    AND TABLE_NAME IN ('CO_WORKER', 'CO_PERSON', 'CO_CUSTOMER', 
                       'CO_EMPLOYMENT_PERIOD', 'CO_SERVICE')
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} BALANCE columns in worker-related tables:")
    for table, column, dtype, length, precision, scale in results:
        type_str = f"{dtype}({precision},{scale})" if precision else f"{dtype}({length})"
        print(f"  {table}.{column} ({type_str})")
else:
    print("No BALANCE columns in worker tables")

# [3] Search for LSL (Long Service Leave) columns
print("\n[3] LSL (Long Service Leave) Columns:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%LSL%' OR COLUMN_NAME LIKE '%LONG_SERVICE%')
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} LSL columns:")
    for table, column, dtype, length, precision, scale in results:
        type_str = f"{dtype}({precision},{scale})" if precision else f"{dtype}({length})"
        print(f"  {table}.{column} ({type_str})")
else:
    print("No LSL columns found")

# [4] Check for leave-related tables
print("\n[4] Leave-Related Tables:")
print("-" * 80)

query = """
    SELECT TABLE_NAME
    FROM ALL_TABLES
    WHERE OWNER = 'SCH_CO_20'
    AND (TABLE_NAME LIKE '%LEAVE%' OR TABLE_NAME LIKE '%LSL%')
    ORDER BY TABLE_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} leave-related tables:")
    for (table_name,) in results:
        print(f"  {table_name}")
        
        # Get key columns
        query_cols = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = 'SCH_CO_20'
            AND TABLE_NAME = :table_name
            AND (COLUMN_NAME LIKE '%BALANCE%' 
                 OR COLUMN_NAME LIKE '%AMOUNT%'
                 OR COLUMN_NAME LIKE '%DAYS%'
                 OR COLUMN_NAME LIKE '%HOURS%'
                 OR COLUMN_NAME LIKE '%CUSTOMER%'
                 OR COLUMN_NAME LIKE '%WORKER%')
            ORDER BY COLUMN_NAME
        """
        cursor.execute(query_cols, {'table_name': table_name})
        cols = cursor.fetchall()
        
        if cols:
            for col, dtype, precision, scale in cols[:10]:
                type_str = f"{dtype}({precision},{scale})" if precision else f"{dtype}"
                print(f"    {col} ({type_str})")
            if len(cols) > 10:
                print(f"    ... and {len(cols) - 10} more columns")
else:
    print("No leave tables found")

# [5] Check code sets for leave types
print("\n[5] Leave-Related Code Sets:")
print("-" * 80)

query = """
    SELECT CODE_SET_ID, CODE_SET_NAME
    FROM CO_CODE_SET
    WHERE UPPER(CODE_SET_NAME) LIKE '%LEAVE%'
    ORDER BY CODE_SET_ID
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} leave code sets:")
    for code_set_id, name in results:
        print(f"  Code Set {code_set_id}: {name}")
else:
    print("No leave code sets found")

# [6] Sample data if CO_LSL_CREDIT exists
print("\n[6] Sample Leave Balance Data:")
print("-" * 80)

# Check if CO_LSL_CREDIT table exists and has balance data
try:
    query = """
        SELECT 
            CUSTOMER_ID,
            LSL_CREDIT_BALANCE,
            LSL_CREDIT_LAST_CALCULATED
        FROM CO_LSL_CREDIT
        WHERE ROWNUM <= 10
        ORDER BY CUSTOMER_ID
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    if results:
        print("CO_LSL_CREDIT sample data:")
        print(f"{'CUSTOMER_ID':<15} {'BALANCE':<20} {'LAST_CALCULATED':<20}")
        print("-" * 57)
        for cust_id, balance, last_calc in results:
            print(f"{str(cust_id):<15} {str(balance):<20} {str(last_calc):<20}")
except Exception as e:
    print(f"CO_LSL_CREDIT table not accessible: {e}")

# [7] Coverage check
print("\n[7] Active Worker Coverage:")
print("-" * 80)

ACTIVE_PERIOD = 202301

try:
    query = f"""
        SELECT 
            COUNT(DISTINCT w.CUSTOMER_ID) as total_workers,
            COUNT(DISTINCT lsl.CUSTOMER_ID) as with_lsl_balance
        FROM CO_WORKER w
        LEFT JOIN CO_LSL_CREDIT lsl ON w.CUSTOMER_ID = lsl.CUSTOMER_ID
        WHERE w.CUSTOMER_ID IN (
            SELECT DISTINCT ep.WORKER_ID
            FROM CO_EMPLOYMENT_PERIOD ep
            INNER JOIN CO_SERVICE s ON s.WORKER = ep.WORKER_ID
            WHERE s.PERIOD_END >= {ACTIVE_PERIOD}
            AND ROWNUM <= 50000
        )
    """
    
    cursor.execute(query)
    result = cursor.fetchone()
    
    if result:
        total, with_lsl = result
        print(f"Total active workers: {total:,}")
        print(f"With LSL balance: {with_lsl:,} ({with_lsl/total*100:.1f}%)")
except Exception as e:
    print(f"Coverage check error: {e}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("SEARCH COMPLETE")
print("=" * 80)
print("\nLikely Mapping:")
print("  Contact.CurrentLeaveBalance__c ← CO_LSL_CREDIT.LSL_CREDIT_BALANCE")
print("  Join: CO_WORKER.CUSTOMER_ID = CO_LSL_CREDIT.CUSTOMER_ID")
