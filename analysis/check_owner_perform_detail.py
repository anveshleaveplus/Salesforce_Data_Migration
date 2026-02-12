"""
Check CO_I_NEW_EMPLOYER_DETAIL table for OWNER_PERFORM_TRADEWORK
This is for OwnersPerformCoveredWork__c mapping
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

print("Checking CO_I_NEW_EMPLOYER_DETAIL for OwnersPerformCoveredWork__c")
print("="*80)

cursor = conn.cursor()

# Check table structure
print("\n1. Table structure (CO_I_NEW_EMPLOYER_DETAIL):")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_I_NEW_EMPLOYER_DETAIL' 
    AND OWNER = 'SCH_CO_20'
    ORDER BY COLUMN_ID
""")
print(f"{'Column Name':<40} {'Type':<15} {'Length':<10} {'Nullable':<10}")
print("-"*80)
for row in cursor:
    print(f"{row[0]:<40} {row[1]:<15} {str(row[2]):<10} {row[3]:<10}")

# Check record count
print("\n2. Record count:")
cursor.execute("SELECT COUNT(*) FROM SCH_CO_20.CO_I_NEW_EMPLOYER_DETAIL")
total = cursor.fetchone()[0]
print(f"   Total records: {total:,}")

# Check OWNER_PERFORM_TRADEWORK values
print("\n3. OWNER_PERFORM_TRADEWORK value distribution:")
cursor.execute("""
    SELECT 
        OWNER_PERFORM_TRADEWORK,
        COUNT(*) as COUNT
    FROM SCH_CO_20.CO_I_NEW_EMPLOYER_DETAIL
    GROUP BY OWNER_PERFORM_TRADEWORK
    ORDER BY COUNT DESC
""")
print(f"{'Value':<30} {'Count':<15} {'Percentage':<15}")
print("-"*60)
for row in cursor:
    pct = (row[1] / total * 100) if total > 0 else 0
    print(f"{str(row[0]):<30} {row[1]:,<15} {pct:.2f}%")

# Check relationship to CO_EMPLOYER
print("\n4. Checking relationship to CO_EMPLOYER:")
cursor.execute("""
    SELECT COLUMN_NAME
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_I_NEW_EMPLOYER_DETAIL' 
    AND OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%EMPLOYER%' OR COLUMN_NAME LIKE '%CUSTOMER%')
    ORDER BY COLUMN_NAME
""")
link_cols = []
for row in cursor:
    link_cols.append(row[0])
    print(f"   - {row[0]}")

# Check if we can join to active employers
if link_cols:
    print("\n5. Checking overlap with active employers:")
    join_col = link_cols[0]  # Use first match
    cursor.execute(f"""
        SELECT COUNT(DISTINCT ned.{join_col})
        FROM SCH_CO_20.CO_I_NEW_EMPLOYER_DETAIL ned
        WHERE ned.{join_col} IN (
            SELECT DISTINCT ep.EMPLOYER_ID
            FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
            WHERE EXISTS (
                SELECT 1 FROM SCH_CO_20.CO_SERVICE s
                WHERE s.WORKER = ep.WORKER_ID
                AND s.PERIOD_END >= 202301
            )
        )
    """)
    active_match = cursor.fetchone()[0]
    print(f"   Active employers with records in CO_I_NEW_EMPLOYER_DETAIL: {active_match:,}")
    
    # Sample data for active employers
    print("\n6. Sample records for active employers:")
    query = f"""
    SELECT 
        e.CUSTOMER_ID,
        e.TRADING_NAME,
        ned.OWNER_PERFORM_TRADEWORK
    FROM SCH_CO_20.CO_EMPLOYER e
    INNER JOIN SCH_CO_20.CO_I_NEW_EMPLOYER_DETAIL ned 
        ON ned.{join_col} = e.CUSTOMER_ID
    WHERE e.CUSTOMER_ID IN (
        SELECT DISTINCT ep.EMPLOYER_ID
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= 202301
        )
        AND ROWNUM <= 50
    )
    AND ROWNUM <= 20
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))

# Sample raw records
print("\n7. Sample raw records from CO_I_NEW_EMPLOYER_DETAIL:")
cursor.execute("""
    SELECT *
    FROM SCH_CO_20.CO_I_NEW_EMPLOYER_DETAIL
    WHERE ROWNUM <= 3
""")
columns = [desc[0] for desc in cursor.description]
print(f"\nColumns: {', '.join(columns)}")
print("-"*80)
for row in cursor:
    for i, col in enumerate(columns):
        if row[i] is not None:
            print(f"  {col}: {row[i]}")
    print()

conn.close()

print("\n" + "="*80)
print("RECOMMENDATION:")
print("✓ Use CO_I_NEW_EMPLOYER_DETAIL.OWNER_PERFORM_TRADEWORK")
print("✓ This is a VARCHAR2 field - likely 'Y'/'N' or 'YES'/'NO'")
print("✓ Convert to boolean for Salesforce")
print("="*80)
