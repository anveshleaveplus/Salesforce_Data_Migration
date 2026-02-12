"""
Search for phone-related columns in CO tables
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

print("Searching for Phone-related columns")
print("="*80)

cursor = conn.cursor()

# Search for phone columns
print("\n1. Phone columns in CO_EMPLOYER:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_EMPLOYER' 
    AND OWNER = 'SCH_CO_20'
    AND COLUMN_NAME LIKE '%PHONE%'
    ORDER BY COLUMN_NAME
""")
found = False
for row in cursor:
    found = True
    print(f"   {row[0]} ({row[1]}, length: {row[2]})")
if not found:
    print("   (No phone columns found in CO_EMPLOYER)")

# Check CO_CUSTOMER
print("\n2. Phone columns in CO_CUSTOMER:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_CUSTOMER' 
    AND OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%PHONE%' OR COLUMN_NAME LIKE '%TELEPHONE%')
    ORDER BY COLUMN_NAME
""")
customer_phone_cols = []
for row in cursor:
    customer_phone_cols.append(row[0])
    print(f"   {row[0]} ({row[1]}, length: {row[2]})")

# Check CO_I_NEW_EMPLOYER_DETAIL
print("\n3. Phone columns in CO_I_NEW_EMPLOYER_DETAIL:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_I_NEW_EMPLOYER_DETAIL' 
    AND OWNER = 'SCH_CO_20'
    AND COLUMN_NAME LIKE '%PHONE%'
    ORDER BY COLUMN_NAME
""")
ned_phone_cols = []
for row in cursor:
    ned_phone_cols.append(row[0])
    print(f"   {row[0]} ({row[1]}, length: {row[2]})")

# Check data population in CO_CUSTOMER
if customer_phone_cols:
    print("\n4. Data population in CO_CUSTOMER (for active employers):")
    ACTIVE_PERIOD = 202301
    
    for col in customer_phone_cols:
        cursor.execute(f"""
            SELECT 
                COUNT(*) as TOTAL,
                COUNT(c.{col}) as WITH_DATA
            FROM SCH_CO_20.CO_EMPLOYER e
            LEFT JOIN SCH_CO_20.CO_CUSTOMER c ON c.CUSTOMER_ID = e.CUSTOMER_ID
            WHERE e.CUSTOMER_ID IN (
                SELECT DISTINCT ep.EMPLOYER_ID
                FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
                WHERE EXISTS (
                    SELECT 1 FROM SCH_CO_20.CO_SERVICE s
                    WHERE s.WORKER = ep.WORKER_ID
                    AND s.PERIOD_END >= {ACTIVE_PERIOD}
                )
            )
        """)
        total, with_data = cursor.fetchone()
        pct = with_data / total * 100 if total > 0 else 0
        print(f"   {col}: {with_data:,} / {total:,} ({pct:.2f}%)")
        
        # Show samples
        if with_data > 0:
            cursor.execute(f"""
                SELECT c.{col}
                FROM SCH_CO_20.CO_CUSTOMER c
                WHERE c.{col} IS NOT NULL
                AND ROWNUM <= 5
            """)
            samples = [row[0] for row in cursor]
            print(f"      Samples: {', '.join(str(s) for s in samples)}")

# Check data in CO_I_NEW_EMPLOYER_DETAIL
if ned_phone_cols:
    print("\n5. Data population in CO_I_NEW_EMPLOYER_DETAIL (for active employers):")
    
    for col in ned_phone_cols:
        cursor.execute(f"""
            SELECT 
                COUNT(*) as TOTAL,
                COUNT(ned.{col}) as WITH_DATA
            FROM SCH_CO_20.CO_EMPLOYER e
            LEFT JOIN SCH_CO_20.CO_I_NEW_EMPLOYER_DETAIL ned ON ned.EMPLOYER_ID = e.CUSTOMER_ID
            WHERE e.CUSTOMER_ID IN (
                SELECT DISTINCT ep.EMPLOYER_ID
                FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
                WHERE EXISTS (
                    SELECT 1 FROM SCH_CO_20.CO_SERVICE s
                    WHERE s.WORKER = ep.WORKER_ID
                    AND s.PERIOD_END >= {ACTIVE_PERIOD}
                )
            )
        """)
        total, with_data = cursor.fetchone()
        pct = with_data / total * 100 if total > 0 else 0
        print(f"   {col}: {with_data:,} / {total:,} ({pct:.2f}%)")

conn.close()

print("\n" + "="*80)
print("RECOMMENDATION:")
print("Best option for Account.Phone:")
if customer_phone_cols:
    print(f"  Use CO_CUSTOMER.{customer_phone_cols[0]} (most populated)")
print("="*80)
