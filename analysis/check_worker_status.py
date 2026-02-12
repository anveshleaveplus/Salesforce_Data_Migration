import os
import sys
import oracledb
import pandas as pd
from dotenv import load_dotenv

# Load environment
load_dotenv('.env.sit')

def connect_oracle():
    """Connect to Oracle database"""
    return oracledb.connect(
        user=os.getenv('ORACLE_USER'),
        password=os.getenv('ORACLE_PASSWORD'),
        host=os.getenv('ORACLE_HOST'),
        port=int(os.getenv('ORACLE_PORT')),
        sid=os.getenv('ORACLE_SID')
    )

conn = connect_oracle()
cursor = conn.cursor()

print("Checking WORKER_STATUS_CODE in CO_WORKER_STATUS")
print("="*80)

# First check CO_WORKER table structure to see if it has status link
print("\nCO_WORKER table structure (checking for status links):")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20' 
    AND TABLE_NAME = 'CO_WORKER'
    ORDER BY COLUMN_ID
""")
worker_columns = cursor.fetchall()
for col in worker_columns:
    nullable = 'NULL' if col[3] == 'Y' else 'NOT NULL'
    print(f"  {col[0]:<30} {col[1]:<15} ({col[2]}) {nullable}")

# Check CO_WORKER_STATUS table structure
print("\nCO_WORKER_STATUS table structure:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20' 
    AND TABLE_NAME = 'CO_WORKER_STATUS'
    ORDER BY COLUMN_ID
""")
columns = cursor.fetchall()
for col in columns:
    nullable = 'NULL' if col[3] == 'Y' else 'NOT NULL'
    print(f"  {col[0]:<30} {col[1]:<15} ({col[2]}) {nullable}")

# Check sample data
query = """
SELECT 
    ws.WORKER_STATUS_ID,
    ws.WORKER_STATUS_CODE,
    ws.WORKER_REASON_CODE,
    ws.IS_SELECTABLE
FROM SCH_CO_20.CO_WORKER_STATUS ws
WHERE ROWNUM <= 100
"""

df = pd.read_sql(query, conn)

print(f"\n✓ Retrieved {len(df)} worker status records")
print(f"\nColumn population:")
print(f"  WORKER_STATUS_CODE not null: {df['WORKER_STATUS_CODE'].notna().sum()}/{len(df)}")

print(f"\nUnique WORKER_STATUS_CODE values:")
status_counts = df['WORKER_STATUS_CODE'].value_counts()
for code, count in status_counts.items():
    print(f"  {code}: {count} ({count/len(df)*100:.1f}%)")

print(f"\nSample records:")
print(df[['WORKER_STATUS_ID', 'WORKER_STATUS_CODE', 'IS_SELECTABLE']].head(15).to_string(index=False))

# Check if CO_WORKER has foreign key to CO_WORKER_STATUS
print("\n" + "="*80)
print("Checking link between CO_WORKER and CO_WORKER_STATUS...")
query_link = """
SELECT 
    w.CUSTOMER_ID,
    w.WORKER_STATUS_ID,
    w.STATUS_EFFECTIVE_FROM_DATE
FROM SCH_CO_20.CO_WORKER w
WHERE w.WORKER_STATUS_ID IS NOT NULL
AND ROWNUM <= 10
"""
try:
    df_link = pd.read_sql(query_link, conn)
    print(f"✓ CO_WORKER.WORKER_STATUS_ID exists - {len(df_link)} sample records")
    print(df_link.to_string(index=False))
    
    # Check if WORKER_STATUS_ID values match WORKER_STATUS table
    print("\nVerifying join and getting status codes for sample workers...")
    query_verify = """
    SELECT 
        w.CUSTOMER_ID,
        w.WORKER_STATUS_ID,
        w.STATUS_EFFECTIVE_FROM_DATE,
        ws.WORKER_STATUS_CODE,
        ws.IS_SELECTABLE
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_WORKER_STATUS ws ON w.WORKER_STATUS_ID = ws.WORKER_STATUS_ID
    WHERE ROWNUM <= 20
    """
    df_verify = pd.read_sql(query_verify, conn)
    print(f"✓ Successfully joined CO_WORKER.WORKER_STATUS_ID to CO_WORKER_STATUS.WORKER_STATUS_ID")
    print(f"  {len(df_verify)} sample records:")
    print(df_verify.to_string(index=False))
    
    # Check status code distribution in CO_WORKER
    print("\n" + "="*80)
    print("Checking WORKER_STATUS_CODE distribution in active workers...")
    query_distribution = """
    SELECT 
        ws.WORKER_STATUS_CODE,
        COUNT(*) as WORKER_COUNT
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_WORKER_STATUS ws ON w.WORKER_STATUS_ID = ws.WORKER_STATUS_ID
    GROUP BY ws.WORKER_STATUS_CODE
    ORDER BY COUNT(*) DESC
    """
    df_dist = pd.read_sql(query_distribution, conn)
    print("Status code distribution across all workers:")
    for _, row in df_dist.iterrows():
        print(f"  {row['WORKER_STATUS_CODE']}: {row['WORKER_COUNT']:,} workers")
except Exception as e:
    print(f"✗ Error: {e}")

# Find the code set for WORKER_STATUS_CODE
print("\n" + "="*80)
print("Looking for WORKER_STATUS_CODE code set...")
cursor.execute("""
    SELECT DISTINCT cs.CODE_SET_ID, cs.CODE_SET_NAME
    FROM SCH_CO_20.CO_CODE_SET cs
    INNER JOIN SCH_CO_20.CO_CODE c ON cs.CODE_SET_ID = c.CODE_SET_ID
    WHERE UPPER(cs.CODE_SET_NAME) LIKE '%WORKER%STATUS%'
       OR UPPER(cs.CODE_SET_NAME) LIKE '%REGISTRATION%'
       OR UPPER(c.DESCRIPTION) LIKE '%WORKER%STATUS%'
       OR UPPER(c.DESCRIPTION) LIKE '%REGISTRATION%'
    ORDER BY cs.CODE_SET_ID
""")
code_sets = cursor.fetchall()

if code_sets:
    print(f"Found {len(code_sets)} potentially related code set(s):")
    for code_set_id, code_set_name in code_sets:
        print(f"\n  Code Set ID: {code_set_id} - {code_set_name}")
        cursor.execute("""
            SELECT VALUE, DESCRIPTION
            FROM SCH_CO_20.CO_CODE
            WHERE CODE_SET_ID = :code_set_id
            ORDER BY VALUE
        """, {'code_set_id': code_set_id})
        codes = cursor.fetchall()
        for code_val, code_desc in codes:
            print(f"    {code_val} -> {code_desc}")

# Also check if there's a direct code set match
print("\n" + "="*80)
print("Searching all code sets for matching status codes (01, 02, 03, 04)...")
status_codes = ['01', '02', '03', '04']

for code_val in status_codes:
    cursor.execute("""
        SELECT DISTINCT cs.CODE_SET_ID, cs.CODE_SET_NAME, c.VALUE, c.DESCRIPTION
        FROM SCH_CO_20.CO_CODE c
        INNER JOIN SCH_CO_20.CO_CODE_SET cs ON c.CODE_SET_ID = cs.CODE_SET_ID
        WHERE c.VALUE = :code_val
    """, {'code_val': code_val})
    matches = cursor.fetchall()
    if matches:
        print(f"\nCode '{code_val}' found in:")
        for code_set_id, code_set_name, code_value, code_desc in matches:
            print(f"  Set {code_set_id} ({code_set_name}): {code_value} -> {code_desc}")

conn.close()
print("\n" + "="*80)
