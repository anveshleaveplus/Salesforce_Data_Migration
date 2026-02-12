"""
Comprehensive search for phone/telephone columns across all CO tables
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

print("Comprehensive Phone Column Search Across All CO Tables")
print("="*80)

cursor = conn.cursor()

# Search ALL tables for phone columns
print("\n1. All phone/telephone columns in SCH_CO_20 schema:")
cursor.execute("""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%PHONE%' OR COLUMN_NAME LIKE '%TELEPHONE%')
    ORDER BY TABLE_NAME, COLUMN_NAME
""")
print(f"{'Table':<40} {'Column':<35} {'Type':<12} {'Length':<6}")
print("-"*100)
all_phone_cols = []
for row in cursor:
    print(f"{row[0]:<40} {row[1]:<35} {row[2]:<12} {row[3]:<6}")
    all_phone_cols.append((row[0], row[1]))

# Check specific employer-related tables
print("\n2. Checking data population for employer-related phone fields:")
ACTIVE_PERIOD = 202301

employer_related_tables = {
    'CO_CUSTOMER': ['MOBILE_PHONE_NO', 'TELEPHONE1_NO', 'TELEPHONE2_NO'],
    'CO_I_NEW_EMPLOYER_DETAIL': ['BUSINESS_PHONE', 'CONTACT_PHONE'],
    'CO_EMPLOYER_CONTACT': []  # Will check if it has phone columns
}

# First check if CO_EMPLOYER_CONTACT has phone columns
cursor.execute("""
    SELECT COLUMN_NAME
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_EMPLOYER_CONTACT'
    AND (COLUMN_NAME LIKE '%PHONE%' OR COLUMN_NAME LIKE '%TELEPHONE%')
""")
employer_contact_phones = [row[0] for row in cursor]
if employer_contact_phones:
    employer_related_tables['CO_EMPLOYER_CONTACT'] = employer_contact_phones

for table, columns in employer_related_tables.items():
    if not columns:
        continue
    
    print(f"\n   {table}:")
    
    for col in columns:
        # Determine join condition
        if table == 'CO_CUSTOMER':
            join_clause = f"LEFT JOIN SCH_CO_20.{table} t ON t.CUSTOMER_ID = e.CUSTOMER_ID"
        elif table == 'CO_I_NEW_EMPLOYER_DETAIL':
            join_clause = f"LEFT JOIN SCH_CO_20.{table} t ON t.EMPLOYER_ID = e.CUSTOMER_ID"
        elif table == 'CO_EMPLOYER_CONTACT':
            join_clause = f"LEFT JOIN SCH_CO_20.{table} t ON t.EMPLOYER_ID = e.CUSTOMER_ID"
        else:
            continue
            
        try:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as TOTAL,
                    COUNT(t.{col}) as WITH_DATA
                FROM SCH_CO_20.CO_EMPLOYER e
                {join_clause}
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
            print(f"      {col:<30} {with_data:>6,} / {total:>6,} ({pct:>6.2f}%)")
            
            # Show sample values if data exists
            if with_data > 0 and with_data < 1000:
                cursor.execute(f"""
                    SELECT DISTINCT t.{col}
                    FROM SCH_CO_20.{table} t
                    WHERE t.{col} IS NOT NULL
                    AND ROWNUM <= 3
                """)
                samples = [str(row[0]) for row in cursor]
                if samples:
                    print(f"         Samples: {', '.join(samples)}")
        except Exception as e:
            print(f"      {col}: Error - {str(e)}")

# Check alternative address/contact tables
print("\n3. Checking other contact-related tables:")
contact_tables = ['CO_ADDRESS', 'CO_CONTACT', 'CO_EMPLOYER_CONTACT', 'CO_PERSON']
for table in contact_tables:
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM ALL_TABLES
        WHERE OWNER = 'SCH_CO_20'
        AND TABLE_NAME = '{table}'
    """)
    if cursor.fetchone()[0] > 0:
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM ALL_TAB_COLUMNS 
            WHERE OWNER = 'SCH_CO_20'
            AND TABLE_NAME = '{table}'
            AND (COLUMN_NAME LIKE '%PHONE%' OR COLUMN_NAME LIKE '%TELEPHONE%')
        """)
        cols = cursor.fetchall()
        if cols:
            print(f"\n   {table}:")
            for col in cols:
                print(f"      - {col[0]} ({col[1]})")

# Look for any columns with 'CONTACT' that might have phone info
print("\n4. Tables with EMPLOYER_ID that might have contact info:")
cursor.execute("""
    SELECT DISTINCT TABLE_NAME
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME = 'EMPLOYER_ID' OR COLUMN_NAME LIKE '%EMPLOYER%')
    AND TABLE_NAME LIKE '%CONTACT%'
    ORDER BY TABLE_NAME
""")
print("   Employer-related contact tables:")
for row in cursor:
    print(f"      - {row[0]}")

conn.close()

print("\n" + "="*80)
print("SEARCH COMPLETE")
print("="*80)
