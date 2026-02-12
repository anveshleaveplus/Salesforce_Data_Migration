"""
Check CO_EMPLOYER.WSR_TYPE_CODE for ReportingStatus__c picklist mapping
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

print("Checking CO_EMPLOYER.WSR_TYPE_CODE for ReportingStatus__c")
print("="*80)

cursor = conn.cursor()

# Check column details
print("\n1. Column details:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_EMPLOYER' 
    AND OWNER = 'SCH_CO_20'
    AND COLUMN_NAME = 'WSR_TYPE_CODE'
""")
result = cursor.fetchone()
if result:
    print(f"   ✓ {result[0]} ({result[1]}, length: {result[2]})")
else:
    print("   ✗ WSR_TYPE_CODE not found")
    conn.close()
    exit(1)

# Check value distribution
ACTIVE_PERIOD = 202301
print("\n2. Value distribution (active employers):")
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
print(f"{'Code':<10} {'Count':<15} {'Percentage':<15}")
print("-"*40)
total = 0
results = []
for row in cursor:
    results.append(row)
    total += row[1]

for row in results:
    pct = row[1] / total * 100 if total > 0 else 0
    print(f"{str(row[0]):<10} {row[1]:,<15} {pct:.2f}%")

# Check code descriptions from CO_CODE table via CO_CODE_SET
print("\n3. Code descriptions from code table:")
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
    print(f"{'Code':<10} {'Description':<50}")
    print("-"*60)
    for row in cursor:
        code_mapping[row[0]] = row[1]
        print(f"{str(row[0]):<10} {row[1]}")
else:
    print("   Code set not found")

# Map codes to Salesforce picklist values
print("\n4. Proposed mapping to Salesforce 'Required'/'Not Required':")
print("-"*60)
print("Based on descriptions, map to:")
for code, desc in code_mapping.items():
    # Determine if this means reporting is required or not
    desc_lower = desc.lower() if desc else ''
    if 'not' in desc_lower or 'exempt' in desc_lower or 'excluded' in desc_lower:
        sf_value = "Not Required"
    elif 'required' in desc_lower or 'report' in desc_lower or 'active' in desc_lower:
        sf_value = "Required"
    else:
        # Need to check description to determine
        sf_value = "???"
    
    print(f"   {code:<10} '{desc}' -> '{sf_value}'")

# Sample records
print("\n5. Sample employers with WSR_TYPE_CODE:")
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
AND ROWNUM <= 20
"""
df = pd.read_sql(query, conn)
# Add description
df['DESCRIPTION'] = df['WSR_TYPE_CODE'].map(code_mapping)
print(df.to_string(index=False))

conn.close()

print("\n" + "="*80)
print("RECOMMENDATION:")
print("Map WSR_TYPE_CODE to ReportingStatus__c based on code descriptions")
print("Transform code values to 'Required' or 'Not Required' picklist values")
print("="*80)
