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

print("Checking Contact address data in CO_CUSTOMER...")
print("="*80)

# Check ADDRESS_ID and POSTAL_ADDRESS_ID columns
query = """
SELECT 
    w.CUSTOMER_ID as WORKER_ID,
    p.PERSON_ID,
    p.FIRST_NAME,
    p.SURNAME,
    c.ADDRESS_ID,
    c.POSTAL_ADDRESS_ID
FROM SCH_CO_20.CO_WORKER w
INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
WHERE ROWNUM <= 100
"""

df = pd.read_sql(query, conn)

print(f"\n✓ Retrieved {len(df)} customer records")
print(f"\nColumn population:")
print(f"  ADDRESS_ID not null: {df['ADDRESS_ID'].notna().sum()}/{len(df)} ({df['ADDRESS_ID'].notna().sum()/len(df)*100:.1f}%)")
print(f"  POSTAL_ADDRESS_ID not null: {df['POSTAL_ADDRESS_ID'].notna().sum()}/{len(df)} ({df['POSTAL_ADDRESS_ID'].notna().sum()/len(df)*100:.1f}%)")

print(f"\nSample ADDRESS_ID values:")
print(df[['FIRST_NAME', 'SURNAME', 'ADDRESS_ID', 'POSTAL_ADDRESS_ID']].head(10).to_string(index=False))

# Check if there's an ADDRESS table
print("\n" + "="*80)
print("Looking for ADDRESS tables...")
cursor.execute("""
    SELECT TABLE_NAME 
    FROM ALL_TABLES 
    WHERE OWNER = 'SCH_CO_20' 
    AND TABLE_NAME LIKE '%ADDRESS%'
    ORDER BY TABLE_NAME
""")
tables = cursor.fetchall()
print(f"Found {len(tables)} address-related tables:")
for table in tables:
    print(f"  - {table[0]}")

# Check structure of CO_ADDRESS if it exists
if any('CO_ADDRESS' in str(t) for t in tables):
    print("\n" + "="*80)
    print("CO_ADDRESS table structure:")
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = 'SCH_CO_20' 
        AND TABLE_NAME = 'CO_ADDRESS'
        ORDER BY COLUMN_ID
    """)
    columns = cursor.fetchall()
    for col in columns:
        nullable = 'NULL' if col[3] == 'Y' else 'NOT NULL'
        print(f"  {col[0]:<30} {col[1]:<15} ({col[2]}) {nullable}")
    
    # Get sample data from CO_ADDRESS
    print("\n" + "="*80)
    print("Sample CO_ADDRESS records:")
    query_addr = """
    SELECT *
    FROM SCH_CO_20.CO_ADDRESS
    WHERE ROWNUM <= 10
    """
    df_addr = pd.read_sql(query_addr, conn)
    print(df_addr.to_string(index=False))

# Check if addresses are populated for our contact records
if any('CO_ADDRESS' in str(t) for t in tables):
    print("\n" + "="*80)
    print("Checking address linkage for contacts...")
    query_linked = """
    SELECT 
        w.CUSTOMER_ID as WORKER_ID,
        p.FIRST_NAME,
        p.SURNAME,
        c.ADDRESS_ID,
        a1.STREET as MAILING_STREET,
        a1.STREET2 as MAILING_STREET2,
        a1.SUBURB as MAILING_SUBURB,
        a1.STATE as MAILING_STATE,
        a1.POSTCODE as MAILING_POSTCODE,
        a1.COUNTRY_CODE as MAILING_COUNTRY,
        c.POSTAL_ADDRESS_ID,
        a2.STREET as POSTAL_STREET,
        a2.STREET2 as POSTAL_STREET2,
        a2.SUBURB as POSTAL_SUBURB,
        a2.STATE as POSTAL_STATE,
        a2.POSTCODE as POSTAL_POSTCODE,
        a2.COUNTRY_CODE as POSTAL_COUNTRY
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
    INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
    LEFT JOIN SCH_CO_20.CO_ADDRESS a1 ON c.ADDRESS_ID = a1.ADDRESS_ID
    LEFT JOIN SCH_CO_20.CO_ADDRESS a2 ON c.POSTAL_ADDRESS_ID = a2.ADDRESS_ID
    WHERE (c.ADDRESS_ID IS NOT NULL OR c.POSTAL_ADDRESS_ID IS NOT NULL)
    AND ROWNUM <= 20
    """
    df_linked = pd.read_sql(query_linked, conn)
    print(f"\n✓ Retrieved {len(df_linked)} contacts with addresses")
    print(df_linked.to_string(index=False))

conn.close()
print("\n" + "="*80)
