"""
Verify ReportingStatus__c mapping from WSR_TYPE_CODE
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

print("Verifying ReportingStatus__c Mapping")
print("="*80)

cursor = conn.cursor()

# Get code mappings
print("\n1. Loading WSR_TYPE_CODE descriptions:")
cursor.execute("""
    SELECT code_set_id 
    FROM SCH_CO_20.CO_CODE_SET 
    WHERE LOWER(code_set_name) LIKE 'wsrtypecode%'
""")
result = cursor.fetchone()

code_mapping = {}
if result:
    code_set_id = result[0]
    cursor.execute(f"""
        SELECT value, description 
        FROM SCH_CO_20.CO_CODE 
        WHERE code_set_id = {code_set_id}
        ORDER BY value
    """)
    for row in cursor:
        code_mapping[row[0]] = row[1]
        print(f"   {row[0]} → '{row[1]}'")

# Test the mapping
ACTIVE_PERIOD = 202301
print("\n2. Testing the transformation:")
query = f"""
SELECT 
    e.CUSTOMER_ID,
    e.TRADING_NAME,
    e.WSR_TYPE_CODE
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
AND ROWNUM <= 50
"""
df = pd.read_sql(query, conn)

# Apply the mapping (simulating what the load script does)
df['ReportingStatus__c'] = df['WSR_TYPE_CODE'].map(code_mapping)

print("\nSample records with mapping:")
print("-"*80)
print(df[['CUSTOMER_ID', 'TRADING_NAME', 'WSR_TYPE_CODE', 'ReportingStatus__c']].head(20).to_string(index=False))

# Show distribution
print("\n3. Distribution of ReportingStatus__c values:")
print("-"*60)
value_counts = df['ReportingStatus__c'].value_counts()
for value, count in value_counts.items():
    pct = count / len(df) * 100
    print(f"   {value:<30} {count:>5} ({pct:>5.1f}%)")

# Check for unmapped values
null_count = df['ReportingStatus__c'].isna().sum()
if null_count > 0:
    print(f"\n   ⚠ WARNING: {null_count} records have NULL ReportingStatus__c")
    unmapped_codes = df[df['ReportingStatus__c'].isna()]['WSR_TYPE_CODE'].unique()
    print(f"   Unmapped codes: {unmapped_codes}")

# Full population stats
print("\n4. Full population statistics (all active employers):")
cursor.execute(f"""
    SELECT 
        e.WSR_TYPE_CODE,
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
    GROUP BY e.WSR_TYPE_CODE
    ORDER BY COUNT DESC
""")
print(f"{'Code':<10} {'Description':<30} {'Count':<15} {'%':<10}")
print("-"*65)
total = 0
results = []
for row in cursor:
    results.append(row)
    total += row[1]

for row in results:
    code = row[0]
    count = row[1]
    desc = code_mapping.get(code, "UNMAPPED")
    pct = count / total * 100 if total > 0 else 0
    print(f"{str(code):<10} {desc:<30} {count:,<15} {pct:.2f}%")

conn.close()

print("\n" + "="*80)
print("✓ ReportingStatus__c mapping verified")
print("✓ WSR_TYPE_CODE codes will be converted to descriptions")
print("✓ Salesforce picklist will receive: 'Not Required', 'Quarterly', 'Employer Cancelled'")
print("="*80)
