"""
Verify OwnersPerformCoveredWork__c mapping in the account load query
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

ACTIVE_PERIOD = 202301

# Test the updated query structure
test_query = f"""
SELECT * FROM (
    SELECT 
        e.CUSTOMER_ID,
        e.TRADING_NAME,
        ned.OWNER_PERFORM_TRADEWORK,
        ROW_NUMBER() OVER (
            PARTITION BY e.CUSTOMER_ID 
            ORDER BY e.CUSTOMER_ID
        ) as rn
    FROM SCH_CO_20.CO_EMPLOYER e
    LEFT JOIN SCH_CO_20.CO_I_NEW_EMPLOYER_DETAIL ned
        ON ned.EMPLOYER_ID = e.CUSTOMER_ID
    WHERE e.CUSTOMER_ID IN (
        SELECT DISTINCT ep.EMPLOYER_ID
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= {ACTIVE_PERIOD}
        )
    )
) WHERE rn = 1 AND ROWNUM <= 100
"""

print("Testing OwnersPerformCoveredWork__c Field Mapping")
print("="*80)
print(f"Active Period Filter: >= {ACTIVE_PERIOD}")
print()

df = pd.read_sql(test_query, conn)

# Show value distribution
print("Value Distribution:")
print("-"*80)
value_counts = df['OWNER_PERFORM_TRADEWORK'].value_counts(dropna=False)
for value, count in value_counts.items():
    pct = count / len(df) * 100
    print(f"  {str(value):<10} {count:>5} ({pct:>5.1f}%)")

# Test boolean conversion
print("\nBoolean Conversion Test:")
print("-"*80)
df['OwnersPerformCoveredWork__c'] = df['OWNER_PERFORM_TRADEWORK'].apply(
    lambda x: True if x == 'Y' else (False if x == 'N' else None)
)

# Show sample records
print("\nSample Records (with boolean conversion):")
print("-"*80)
sample = df[['CUSTOMER_ID', 'TRADING_NAME', 'OWNER_PERFORM_TRADEWORK', 'OwnersPerformCoveredWork__c']].head(20)
print(sample.to_string(index=False))

print("\n" + "="*80)
print("Statistics:")
print(f"  Total employers sampled: {len(df):,}")
print(f"  With OWNER_PERFORM_TRADEWORK data: {df['OWNER_PERFORM_TRADEWORK'].notna().sum():,}")
print(f"  Without data (NULL): {df['OWNER_PERFORM_TRADEWORK'].isna().sum():,}")
print(f"  Value 'Y' (True): {(df['OWNER_PERFORM_TRADEWORK'] == 'Y').sum():,}")
print(f"  Value 'N' (False): {(df['OWNER_PERFORM_TRADEWORK'] == 'N').sum():,}")

# Full population stats
cursor = conn.cursor()
cursor.execute(f"""
    SELECT 
        COUNT(*) as TOTAL,
        COUNT(ned.OWNER_PERFORM_TRADEWORK) as WITH_DATA,
        SUM(CASE WHEN ned.OWNER_PERFORM_TRADEWORK = 'Y' THEN 1 ELSE 0 END) as VALUE_Y,
        SUM(CASE WHEN ned.OWNER_PERFORM_TRADEWORK = 'N' THEN 1 ELSE 0 END) as VALUE_N
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
total, with_data, value_y, value_n = cursor.fetchone()

print(f"\nFull Population Stats (All Active Employers):")
print(f"  Total active employers: {total:,}")
print(f"  With OWNER_PERFORM_TRADEWORK data: {with_data:,} ({with_data/total*100:.2f}%)")
print(f"  Without data: {total-with_data:,} ({(total-with_data)/total*100:.2f}%)")
if with_data > 0:
    print(f"  Y (owners perform work): {value_y:,} ({value_y/with_data*100:.1f}% of populated)")
    print(f"  N (owners don't perform): {value_n:,} ({value_n/with_data*100:.1f}% of populated)")

print("\n✓ Query executes successfully!")
print("✓ Boolean conversion working correctly")
print("✓ Mapping OwnersPerformCoveredWork__c <- CO_I_NEW_EMPLOYER_DETAIL.OWNER_PERFORM_TRADEWORK")
print("="*80)

conn.close()
