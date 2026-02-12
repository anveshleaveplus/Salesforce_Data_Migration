"""
Check CO_EMPLOYER for LEAVEPLUS_REGISTRATION_DATE column
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

print("Checking CO_EMPLOYER for Registration Date Columns")
print("="*80)

cursor = conn.cursor()

# Check for LEAVEPLUS_REGISTRATION_DATE
print("\n1. Checking for LEAVEPLUS_REGISTRATION_DATE column:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_EMPLOYER' 
    AND OWNER = 'SCH_CO_20'
    AND COLUMN_NAME = 'LEAVEPLUS_REGISTRATION_DATE'
""")
result = cursor.fetchone()
if result:
    print(f"   ✓ Column exists: {result[0]} ({result[1]})")
else:
    print("   ✗ LEAVEPLUS_REGISTRATION_DATE not found")

# Search for registration-related date columns
print("\n2. All registration-related date columns in CO_EMPLOYER:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_EMPLOYER' 
    AND OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%REGIST%' OR COLUMN_NAME LIKE '%DATE%')
    ORDER BY COLUMN_NAME
""")
date_cols = []
print(f"{'Column Name':<45} {'Type':<15} {'Length':<10}")
print("-"*70)
for row in cursor:
    print(f"{row[0]:<45} {row[1]:<15} {str(row[2]):<10}")
    if 'DATE' in row[0] or row[1] == 'DATE':
        date_cols.append(row[0])

# Check data population for date columns
if date_cols:
    print("\n3. Checking data population for date columns (active employers):")
    ACTIVE_PERIOD = 202301
    
    for col in date_cols[:10]:  # Limit to first 10
        try:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as TOTAL,
                    COUNT(e.{col}) as WITH_DATA
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
            """)
            total, with_data = cursor.fetchone()
            pct = with_data / total * 100 if total > 0 else 0
            print(f"   {col:<45} {with_data:>6,} / {total:>6,} ({pct:>6.2f}%)")
            
            # Show sample dates
            if with_data > 0 and with_data < 50000:
                cursor.execute(f"""
                    SELECT TO_CHAR(MIN({col}), 'YYYY-MM-DD'), TO_CHAR(MAX({col}), 'YYYY-MM-DD'), COUNT(DISTINCT {col})
                    FROM SCH_CO_20.CO_EMPLOYER
                    WHERE {col} IS NOT NULL
                    AND {col} > TO_DATE('1900-01-01', 'YYYY-MM-DD')
                """)
                min_date, max_date, distinct_count = cursor.fetchone()
                if min_date:
                    print(f"      Range: {min_date} to {max_date}, Distinct values: {distinct_count:,}")
        except Exception as e:
            print(f"   {col:<45} Error: {str(e)[:50]}")

# Sample records with registration dates
print("\n4. Sample employers with populated date fields:")
sample_cols = [col for col in date_cols[:5] if col]
if sample_cols:
    col_list = ', '.join([f"TO_CHAR(e.{col}, 'YYYY-MM-DD') as {col}" for col in sample_cols])
    query = f"""
    SELECT 
        e.CUSTOMER_ID,
        e.TRADING_NAME,
        {col_list}
    FROM SCH_CO_20.CO_EMPLOYER e
    WHERE e.CUSTOMER_ID IN (
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
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))

# Check CO_WSR_SERVICE for employment start date
print("\n5. Reminder: We already use EMPLOYMENT_START_DATE from CO_WSR_SERVICE:")
print("   Mapped to: DateEmploymentCommenced__c")

conn.close()

print("\n" + "="*80)
print("RECOMMENDATION:")
print("Check which date field best represents 'registration date' for the employer")
print("="*80)
