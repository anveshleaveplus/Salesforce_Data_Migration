"""
Search for columns that contain 'Required' or 'Not Required' values
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

print("Searching for columns with 'Required' or 'Not Required' values")
print("="*80)

cursor = conn.cursor()

# Get all CO_EMPLOYER columns
print("\n1. Checking all CO_EMPLOYER columns for 'Required'/'Not Required' values:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_EMPLOYER' 
    AND OWNER = 'SCH_CO_20'
    AND DATA_TYPE IN ('VARCHAR2', 'CHAR')
    ORDER BY COLUMN_NAME
""")
employer_columns = [row[0] for row in cursor.fetchall()]
print(f"   Found {len(employer_columns)} text columns in CO_EMPLOYER")

# Check each column for Required/Not Required values
print("\n2. Checking which columns contain 'Required' values:")
ACTIVE_PERIOD = 202301
found_columns = []

for col in employer_columns:
    try:
        cursor.execute(f"""
            SELECT DISTINCT e.{col}
            FROM SCH_CO_20.CO_EMPLOYER e
            WHERE e.CUSTOMER_ID IN (
                SELECT DISTINCT ep.EMPLOYER_ID
                FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
                WHERE EXISTS (
                    SELECT 1 FROM SCH_CO_20.CO_SERVICE s
                    WHERE s.WORKER = ep.WORKER_ID
                    AND s.PERIOD_END >= {ACTIVE_PERIOD}
                )
            )
            AND (
                UPPER(e.{col}) LIKE '%REQUIRED%'
                OR UPPER(e.{col}) LIKE '%NOT%REQUIRED%'
            )
            AND ROWNUM <= 10
        """)
        results = cursor.fetchall()
        if results:
            found_columns.append(col)
            print(f"\n   ✓ {col}:")
            for row in results:
                print(f"      - {row[0]}")
    except Exception as e:
        # Skip columns that cause errors
        pass

if not found_columns:
    print("   (No columns found with 'Required' values)")

# Check for columns with 'REPORT' or 'STATUS' in name
print("\n3. Columns with 'REPORT' or 'STATUS' in name:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_EMPLOYER' 
    AND OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%REPORT%' OR COLUMN_NAME LIKE '%STATUS%')
    ORDER BY COLUMN_NAME
""")
report_status_cols = []
for row in cursor:
    report_status_cols.append(row[0])
    print(f"   {row[0]} ({row[1]}, length: {row[2]})")

# Check their values
if report_status_cols:
    print("\n4. Values in REPORT/STATUS columns:")
    for col in report_status_cols:
        try:
            cursor.execute(f"""
                SELECT 
                    e.{col},
                    COUNT(*) as COUNT
                FROM SCH_CO_20.CO_EMPLOYER e
                WHERE e.CUSTOMER_ID IN (
                    SELECT DISTINCT ep.EMPLOYER_ID
                    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
                    WHERE EXISTS (
                        SELECT 1 FROM SCH_CO_20.CO_SERVICE s
                        WHERE s.WORKER = ep.WORKER_ID
                        AND s.PERIOD_END >= {ACTIVE_PERIOD}
                    )
                )
                GROUP BY e.{col}
                ORDER BY COUNT DESC
            """)
            print(f"\n   {col}:")
            results = cursor.fetchall()
            if results:
                for row in results[:10]:  # Show top 10 values
                    pct = row[1] / 53857 * 100 if row[1] else 0
                    print(f"      {str(row[0]):<30} {row[1]:>6,} ({pct:>5.1f}%)")
            else:
                print("      (No data)")
        except Exception as e:
            print(f"      Error: {str(e)[:50]}")

# Search across all CO tables
print("\n5. Searching all CO tables for 'REPORTING' columns:")
cursor.execute("""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME LIKE 'CO_%'
    AND COLUMN_NAME LIKE '%REPORT%'
    AND TABLE_NAME NOT LIKE '%_JN'
    AND TABLE_NAME NOT LIKE '%_V'
    ORDER BY TABLE_NAME, COLUMN_NAME
""")
print(f"{'Table':<40} {'Column':<40} {'Type':<15}")
print("-"*95)
reporting_cols = []
for row in cursor:
    reporting_cols.append((row[0], row[1]))
    print(f"{row[0]:<40} {row[1]:<40} {row[2]:<15}")

if not reporting_cols:
    print("   (None found)")

# Check if any of these reporting columns have the Required/Not Required values
if reporting_cols:
    print("\n6. Checking reporting columns for 'Required' values:")
    for table, col in reporting_cols[:10]:  # Check first 10
        try:
            cursor.execute(f"""
                SELECT DISTINCT {col}
                FROM SCH_CO_20.{table}
                WHERE ROWNUM <= 10
            """)
            values = [str(row[0]) for row in cursor.fetchall() if row[0] is not None]
            if any('REQUIRED' in str(v).upper() for v in values):
                print(f"\n   ✓ {table}.{col}:")
                for v in values:
                    print(f"      - {v}")
        except:
            pass

conn.close()

print("\n" + "="*80)
print("SEARCH COMPLETE")
print("="*80)
