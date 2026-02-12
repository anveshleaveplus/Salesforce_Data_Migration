"""
Check CO_CONTACT table for employer phone numbers
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

print("Checking CO_CONTACT Table for Employer Phone Numbers")
print("="*80)

cursor = conn.cursor()

# Check CO_CONTACT table structure
print("\n1. CO_CONTACT table structure:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_CONTACT' 
    AND OWNER = 'SCH_CO_20'
    ORDER BY COLUMN_ID
""")
print(f"{'Column':<40} {'Type':<15} {'Length':<8} {'Nullable':<10}")
print("-"*80)
for row in cursor:
    print(f"{row[0]:<40} {row[1]:<15} {str(row[2]):<8} {row[3]:<10}")

# Check record count
print("\n2. Record count:")
cursor.execute("SELECT COUNT(*) FROM SCH_CO_20.CO_CONTACT")
total = cursor.fetchone()[0]
print(f"   Total records in CO_CONTACT: {total:,}")

# Check how to link to employers
print("\n3. Columns that link to employers:")
cursor.execute("""
    SELECT COLUMN_NAME
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_CONTACT' 
    AND OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%CUSTOMER%' OR COLUMN_NAME LIKE '%EMPLOYER%')
    ORDER BY COLUMN_NAME
""")
link_cols = []
for row in cursor:
    link_cols.append(row[0])
    print(f"   - {row[0]}")

if not link_cols:
    print("   (No direct employer link columns found)")
    print("   Checking for indirect relationships...")

# Check phone data in CO_CONTACT
print("\n4. Phone data population in CO_CONTACT:")
cursor.execute("""
    SELECT 
        COUNT(*) as TOTAL,
        COUNT(TELEPHONE1_NO) as HAS_TELEPHONE1,
        COUNT(TELEPHONE2_NO) as HAS_TELEPHONE2,
        COUNT(MOBILE_PHONE_NO) as HAS_MOBILE
    FROM SCH_CO_20.CO_CONTACT
""")
result = cursor.fetchone()
print(f"   Total records: {result[0]:,}")
print(f"   With TELEPHONE1_NO: {result[1]:,} ({result[1]/result[0]*100:.2f}%)")
print(f"   With TELEPHONE2_NO: {result[2]:,} ({result[2]/result[0]*100:.2f}%)")
print(f"   With MOBILE_PHONE_NO: {result[3]:,} ({result[3]/result[0]*100:.2f}%)")

# Check if CO_CONTACT links through CUSTOMER_ID
if 'CUSTOMER_ID' in link_cols:
    ACTIVE_PERIOD = 202301
    print("\n5. Checking CO_CONTACT data for active employers (via CUSTOMER_ID):")
    
    cursor.execute(f"""
        SELECT 
            COUNT(DISTINCT c.CUSTOMER_ID) as ACTIVE_EMPLOYERS_WITH_CONTACTS,
            COUNT(*) as TOTAL_CONTACT_RECORDS,
            COUNT(c.TELEPHONE1_NO) as WITH_TELEPHONE1,
            COUNT(c.MOBILE_PHONE_NO) as WITH_MOBILE
        FROM SCH_CO_20.CO_CONTACT c
        WHERE c.CUSTOMER_ID IN (
            SELECT DISTINCT ep.EMPLOYER_ID
            FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
            WHERE EXISTS (
                SELECT 1 FROM SCH_CO_20.CO_SERVICE s
                WHERE s.WORKER = ep.WORKER_ID
                AND s.PERIOD_END >= {ACTIVE_PERIOD}
            )
        )
    """)
    result = cursor.fetchone()
    print(f"   Active employers with contact records: {result[0]:,}")
    print(f"   Total contact records: {result[1]:,}")
    print(f"   With TELEPHONE1_NO: {result[2]:,} ({result[2]/result[1]*100:.2f}% of contacts)")
    print(f"   With MOBILE_PHONE_NO: {result[3]:,} ({result[3]/result[1]*100:.2f}% of contacts)")
    
    # Sample data
    print("\n6. Sample contacts for active employers:")
    query = f"""
    SELECT 
        e.CUSTOMER_ID,
        e.TRADING_NAME,
        c.TELEPHONE1_NO,
        c.TELEPHONE2_NO,
        c.MOBILE_PHONE_NO
    FROM SCH_CO_20.CO_EMPLOYER e
    INNER JOIN SCH_CO_20.CO_CONTACT c ON c.CUSTOMER_ID = e.CUSTOMER_ID
    WHERE e.CUSTOMER_ID IN (
        SELECT DISTINCT ep.EMPLOYER_ID
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= {ACTIVE_PERIOD}
        )
    )
    AND (c.TELEPHONE1_NO IS NOT NULL OR c.MOBILE_PHONE_NO IS NOT NULL)
    AND ROWNUM <= 20
    """
    df = pd.read_sql(query, conn)
    if len(df) > 0:
        print(df.to_string(index=False))
    else:
        print("   No contacts with phone numbers found for active employers")

# Check EMPLOYER_V view
print("\n7. Checking EMPLOYER_V view (might already have phone data):")
cursor.execute("""
    SELECT COUNT(*)
    FROM ALL_VIEWS
    WHERE OWNER = 'SCH_CO_20'
    AND VIEW_NAME = 'EMPLOYER_V'
""")
if cursor.fetchone()[0] > 0:
    cursor.execute(f"""
        SELECT 
            COUNT(*) as TOTAL,
            COUNT(TELEPHONE1_NO) as HAS_TELEPHONE1,
            COUNT(TELEPHONE2_NO) as HAS_TELEPHONE2,
            COUNT(MOBILE_PHONE_NO) as HAS_MOBILE
        FROM SCH_CO_20.EMPLOYER_V
        WHERE CUSTOMER_ID IN (
            SELECT DISTINCT ep.EMPLOYER_ID
            FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
            WHERE EXISTS (
                SELECT 1 FROM SCH_CO_20.CO_SERVICE s
                WHERE s.WORKER = ep.WORKER_ID
                AND s.PERIOD_END >= {ACTIVE_PERIOD}
            )
        )
    """)
    result = cursor.fetchone()
    print(f"   Active employers: {result[0]:,}")
    print(f"   With TELEPHONE1_NO: {result[1]:,} ({result[1]/result[0]*100:.2f}%)")
    print(f"   With TELEPHONE2_NO: {result[2]:,} ({result[2]/result[0]*100:.2f}%)")
    print(f"   With MOBILE_PHONE_NO: {result[3]:,} ({result[3]/result[0]*100:.2f}%)")
    
    # Sample
    if result[1] > 0 or result[3] > 0:
        print("\n   Sample phone numbers from EMPLOYER_V:")
        cursor.execute(f"""
            SELECT CUSTOMER_ID, TRADING_NAME, TELEPHONE1_NO, MOBILE_PHONE_NO
            FROM SCH_CO_20.EMPLOYER_V
            WHERE (TELEPHONE1_NO IS NOT NULL OR MOBILE_PHONE_NO IS NOT NULL)
            AND CUSTOMER_ID IN (
                SELECT DISTINCT ep.EMPLOYER_ID
                FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
                WHERE EXISTS (
                    SELECT 1 FROM SCH_CO_20.CO_SERVICE s
                    WHERE s.WORKER = ep.WORKER_ID
                    AND s.PERIOD_END >= {ACTIVE_PERIOD}
                )
                AND ROWNUM <= 100
            )
            AND ROWNUM <= 10
        """)
        for row in cursor:
            print(f"      {row[0]}: {row[1][:40]:<40} Tel: {row[2] or 'NULL':<15} Mobile: {row[3] or 'NULL'}")

conn.close()

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
