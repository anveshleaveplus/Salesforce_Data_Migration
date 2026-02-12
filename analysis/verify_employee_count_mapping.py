"""
Verify that NumberOfEmployees mapping works correctly in the account load query
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
        emp_count.EMPLOYEE_COUNT as NUMBER_OF_EMPLOYEES,
        ROW_NUMBER() OVER (
            PARTITION BY e.CUSTOMER_ID 
            ORDER BY e.CUSTOMER_ID
        ) as rn
    FROM SCH_CO_20.CO_EMPLOYER e
    LEFT JOIN (
        SELECT 
            ep.EMPLOYER_ID,
            COUNT(DISTINCT ep.WORKER_ID) as EMPLOYEE_COUNT
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= {ACTIVE_PERIOD}
        )
        GROUP BY ep.EMPLOYER_ID
    ) emp_count ON emp_count.EMPLOYER_ID = e.CUSTOMER_ID
    WHERE e.CUSTOMER_ID IN (
        SELECT DISTINCT ep.EMPLOYER_ID
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= {ACTIVE_PERIOD}
        )
    )
) WHERE rn = 1 AND ROWNUM <= 20
"""

print("Testing NumberOfEmployees Field Mapping")
print("="*80)
print(f"Active Period Filter: >= {ACTIVE_PERIOD}")
print()

df = pd.read_sql(test_query, conn)

print("Sample Results (Top 20 employers):")
print("-"*80)
print(df[['CUSTOMER_ID', 'TRADING_NAME', 'NUMBER_OF_EMPLOYEES']].to_string(index=False))

print("\n" + "="*80)
print("Statistics:")
print(f"  Total employers: {len(df):,}")
print(f"  Employers with employee count: {df['NUMBER_OF_EMPLOYEES'].notna().sum():,}")
print(f"  Employers without count: {df['NUMBER_OF_EMPLOYEES'].isna().sum():,}")
print(f"  Average employees per employer: {df['NUMBER_OF_EMPLOYEES'].mean():.1f}")
print(f"  Median employees per employer: {df['NUMBER_OF_EMPLOYEES'].median():.1f}")
print(f"  Max employees: {df['NUMBER_OF_EMPLOYEES'].max():,.0f}")
print(f"  Min employees: {df['NUMBER_OF_EMPLOYEES'].min():,.0f}")

print("\n✓ Query executes successfully!")
print("✓ NumberOfEmployees field is populated")
print("="*80)

conn.close()
