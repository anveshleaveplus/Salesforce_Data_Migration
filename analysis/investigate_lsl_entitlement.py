"""
Investigate CO_WORKER_ENTITLEMENT_V4 and related tables for leave balance
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
print("LEAVE ENTITLEMENT TABLES INVESTIGATION")
print("=" * 80)

# [1] Check CO_WORKER_ENTITLEMENT_V4 structure
print("\n[1] CO_WORKER_ENTITLEMENT_V4 Structure:")
print("-" * 80)

try:
    query = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = 'SCH_CO_20'
        AND TABLE_NAME = 'CO_WORKER_ENTITLEMENT_V4'
        AND COLUMN_NAME LIKE '%LSL_DUE%'
        ORDER BY COLUMN_NAME
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    if results:
        print(f"Found {len(results)} LSL_DUE columns:")
        for col, dtype, precision, scale in results:
            type_str = f"{dtype}({precision},{scale})" if precision else dtype
            print(f"  {col} ({type_str})")
    else:
        print("No LSL_DUE columns found")
except Exception as e:
    print(f"Error: {e}")

# [2] Sample data from CO_WORKER_ENTITLEMENT_V4
print("\n[2] Sample CO_WORKER_ENTITLEMENT_V4 Data:")
print("-" * 80)

try:
    query = """
        SELECT 
            CUSTOMER_ID,
            LSL_DAYS,
            LSL_DUE,
            LSL_DUE_WRK,
            LSL_DUE_WSC
        FROM CO_WORKER_ENTITLEMENT_V4
        WHERE ROWNUM <= 10
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    if results:
        print(f"{'CUSTOMER_ID':<12} {'LSL_DAYS':<12} {'LSL_DUE':<12} {'DUE_WRK':<12} {'DUE_WSC':<12}")
        print("-" * 62)
        for cust_id, days, due, due_wrk, due_wsc in results:
            print(f"{str(cust_id):<12} {str(days):<12} {str(due):<12} {str(due_wrk):<12} {str(due_wsc):<12}")
    else:
        print("No data found")
except Exception as e:
    print(f"Table not accessible: {e}")

# [3] Check CO_WORKER_ENTIT_V4_TODAY (most current)
print("\n[3] CO_WORKER_ENTIT_V4_TODAY Sample Data:")
print("-" * 80)

try:
    query = """
        SELECT 
            CUSTOMER_ID,
            LSL_DAYS,
            LSL_DUE,
            LSL_WORKER_DAYS_TAKEN,
            LSL_WSC_DAYS_TAKEN
        FROM CO_WORKER_ENTIT_V4_TODAY
        WHERE LSL_DUE IS NOT NULL
        AND ROWNUM <= 10
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    if results:
        print(f"{'CUSTOMER_ID':<12} {'LSL_DAYS':<12} {'LSL_DUE':<12} {'TAKEN_WRK':<12} {'TAKEN_WSC':<12}")
        print("-" * 62)
        for cust_id, days, due, taken_wrk, taken_wsc in results:
            print(f"{str(cust_id):<12} {str(days):<12} {str(due):<12} {str(taken_wrk):<12} {str(taken_wsc):<12}")
    else:
        print("No data with LSL_DUE found")
except Exception as e:
    print(f"Table not accessible: {e}")

# [4] Coverage check - active workers
print("\n[4] Active Worker LSL Coverage:")
print("-" * 80)

ACTIVE_PERIOD = 202301

try:
    query = f"""
        SELECT 
            COUNT(DISTINCT w.CUSTOMER_ID) as total_workers,
            COUNT(DISTINCT CASE WHEN ent.LSL_DUE IS NOT NULL THEN w.CUSTOMER_ID END) as with_lsl_due,
            COUNT(DISTINCT CASE WHEN ent.LSL_DUE > 0 THEN w.CUSTOMER_ID END) as with_positive_lsl
        FROM (
            SELECT DISTINCT ep.WORKER_ID as CUSTOMER_ID
            FROM CO_EMPLOYMENT_PERIOD ep
            INNER JOIN CO_SERVICE s ON s.WORKER = ep.WORKER_ID
            WHERE s.PERIOD_END >= {ACTIVE_PERIOD}
            AND ROWNUM <= 50000
        ) w
        LEFT JOIN CO_WORKER_ENTIT_V4_TODAY ent ON w.CUSTOMER_ID = ent.CUSTOMER_ID
    """
    
    cursor.execute(query)
    result = cursor.fetchone()
    
    if result:
        total, with_due, with_positive = result
        print(f"Total active workers: {total:,}")
        print(f"With LSL_DUE (not null): {with_due:,} ({with_due/total*100:.1f}%)")
        print(f"With LSL_DUE > 0: {with_positive:,} ({with_positive/total*100:.1f}%)")
except Exception as e:
    print(f"Coverage check error: {e}")

# [5] Check value ranges
print("\n[5] LSL_DUE Value Ranges (Active Workers):")
print("-" * 80)

try:
    query = f"""
        SELECT 
            MIN(ent.LSL_DUE) as min_lsl,
            MAX(ent.LSL_DUE) as max_lsl,
            AVG(ent.LSL_DUE) as avg_lsl,
            MEDIAN(ent.LSL_DUE) as median_lsl
        FROM (
            SELECT DISTINCT ep.WORKER_ID as CUSTOMER_ID
            FROM CO_EMPLOYMENT_PERIOD ep
            INNER JOIN CO_SERVICE s ON s.WORKER = ep.WORKER_ID
            WHERE s.PERIOD_END >= {ACTIVE_PERIOD}
            AND ROWNUM <= 50000
        ) w
        INNER JOIN CO_WORKER_ENTIT_V4_TODAY ent ON w.CUSTOMER_ID = ent.CUSTOMER_ID
        WHERE ent.LSL_DUE IS NOT NULL
    """
    
    cursor.execute(query)
    result = cursor.fetchone()
    
    if result:
        min_lsl, max_lsl, avg_lsl, median_lsl = result
        print(f"Min LSL_DUE: {min_lsl}")
        print(f"Max LSL_DUE: {max_lsl}")
        print(f"Avg LSL_DUE: {avg_lsl:.2f}")
        print(f"Median LSL_DUE: {median_lsl:.2f}")
except Exception as e:
    print(f"Range check error: {e}")

# [6] Check CO_WORKER_SOS table
print("\n[6] CO_WORKER_SOS (Statement of Service) Investigation:")
print("-" * 80)

try:
    query = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = 'SCH_CO_20'
        AND TABLE_NAME = 'CO_WORKER_SOS'
        AND (COLUMN_NAME LIKE '%LSL%' OR COLUMN_NAME = 'CUSTOMER_ID')
        ORDER BY COLUMN_NAME
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    if results:
        print(f"Found {len(results)} LSL columns in CO_WORKER_SOS:")
        for col, dtype, precision, scale in results:
            type_str = f"{dtype}({precision},{scale})" if precision else dtype
            print(f"  {col} ({type_str})")
        
        # Sample data
        print("\n  Sample CO_WORKER_SOS data:")
        query_sample = """
            SELECT 
                CUSTOMER_ID,
                LSL_AVAILABLE_IN_WEEK,
                LSL_TAKEN_IN_WEEK,
                TOTAL_LSL_ENT_IN_WEEK
            FROM CO_WORKER_SOS
            WHERE ROWNUM <= 5
        """
        cursor.execute(query_sample)
        samples = cursor.fetchall()
        
        if samples:
            print(f"  {'CUSTOMER_ID':<12} {'AVAILABLE':<12} {'TAKEN':<12} {'TOTAL_ENT':<12}")
            print(f"  {'-'*50}")
            for row in samples:
                print(f"  {str(row[0]):<12} {str(row[1]):<12} {str(row[2]):<12} {str(row[3]):<12}")
    else:
        print("CO_WORKER_SOS table not found or no LSL columns")
except Exception as e:
    print(f"Error: {e}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("RECOMMENDATION")
print("=" * 80)
print("\nContact.CurrentLeaveBalance__c (Number 18,0)")
print("  ← CO_WORKER_ENTIT_V4_TODAY.LSL_DUE (NUMBER 7,2)")
print("\nJoin: CO_WORKER.CUSTOMER_ID = CO_WORKER_ENTIT_V4_TODAY.CUSTOMER_ID")
print("\nNote:")
print("  - LSL_DUE is in DAYS (decimal with 2 places)")
print("  - SF field is Number(18,0) - integer only")
print("  - Will need to ROUND() to convert days to whole number")
print("  - Check coverage percentage for active workers")
