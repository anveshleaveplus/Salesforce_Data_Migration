"""
Pre-load validation checks for Account data
- Check for duplicates
- Validate data types
- Check field lengths
- Validate picklist values
"""
import oracledb
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv('.env.sit')

print("="*100)
print("ACCOUNT DATA VALIDATION - PRE-LOAD CHECKS")
print("="*100)

# Connect to Oracle
conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

ACTIVE_PERIOD = 202301

# Get the data (same query as load script)
print("\n1. Extracting data...")
query = f"""
WITH base_data AS (
    SELECT 
        e.CUSTOMER_ID,
        e.TRADING_NAME,
        e.ABN,
        e.ACN,
        e.EMPLOYER_TYPE_CODE,
        addr.STREET as REG_STREET,
        addr.STREET2 as REG_STREET2,
        addr.SUBURB as REG_SUBURB,
        addr.STATE as REG_STATE,
        addr.POSTCODE as REG_POSTCODE,
        addr.COUNTRY_CODE as REG_COUNTRY,
        CASE 
            WHEN c.POSTAL_ADDRESS_ID IS NOT NULL 
                AND c.ADDRESS_ID != c.POSTAL_ADDRESS_ID 
            THEN 1 
            ELSE 0 
        END as IS_POSTAL_DIFFERENT,
        postal_addr.STREET as POSTAL_STREET,
        postal_addr.STREET2 as POSTAL_STREET2,
        postal_addr.SUBURB as POSTAL_SUBURB,
        postal_addr.STATE as POSTAL_STATE,
        postal_addr.POSTCODE as POSTAL_POSTCODE,
        postal_addr.COUNTRY_CODE as POSTAL_COUNTRY,
        ws.EMPLOYMENT_START_DATE,
        c.EMAIL_ADDRESS,
        ned.OWNER_PERFORM_TRADEWORK,
        emp_count.EMPLOYEE_COUNT as NUMBER_OF_EMPLOYEES,
        ROW_NUMBER() OVER (
            PARTITION BY e.CUSTOMER_ID 
            ORDER BY ws.EMPLOYMENT_START_DATE DESC NULLS LAST
        ) as rn
    FROM SCH_CO_20.CO_EMPLOYER e
    LEFT JOIN SCH_CO_20.CO_CUSTOMER c
        ON c.CUSTOMER_ID = e.CUSTOMER_ID
    LEFT JOIN SCH_CO_20.CO_ADDRESS addr
        ON addr.ADDRESS_ID = c.ADDRESS_ID
    LEFT JOIN SCH_CO_20.CO_ADDRESS postal_addr
        ON postal_addr.ADDRESS_ID = c.POSTAL_ADDRESS_ID
    LEFT JOIN SCH_CO_20.CO_WSR_SERVICE ws 
        ON ws.WSR_ID = e.CUSTOMER_ID
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
    AND e.CUSTOMER_ID != 23000  -- Exclude "LONG SERVICE LEAVE CREDITS" (internal LP account)
)
SELECT * FROM base_data WHERE rn = 1
"""

df = pd.read_sql(query, conn)
print(f"   ✓ Extracted {len(df):,} records")

# Get picklist mappings
cursor = conn.cursor()
cursor.execute("""
    SELECT code_set_id 
    FROM SCH_CO_20.CO_CODE_SET 
    WHERE LOWER(code_set_name) LIKE 'employertypecode%'
""")
result = cursor.fetchone()
employer_type_mapping = {}
if result:
    cursor.execute(f"""
        SELECT value, description 
        FROM SCH_CO_20.CO_CODE 
        WHERE code_set_id = {result[0]}
    """)
    for row in cursor:
        employer_type_mapping[row[0]] = row[1]

conn.close()

print("\n" + "="*100)
print("VALIDATION CHECKS")
print("="*100)

issues = []

# 1. CHECK FOR DUPLICATES
print("\n1. DUPLICATE CHECK - External_Id__c (CUSTOMER_ID)")
print("-"*100)
dupes = df[df.duplicated(subset=['CUSTOMER_ID'], keep=False)]
if len(dupes) > 0:
    print(f"   ⚠ CRITICAL: Found {len(dupes)} duplicate CUSTOMER_IDs")
    print(dupes[['CUSTOMER_ID', 'TRADING_NAME']].to_string(index=False))
    issues.append(f"CRITICAL: {len(dupes)} duplicate CUSTOMER_IDs found")
else:
    print(f"   ✓ No duplicates - all {len(df):,} CUSTOMER_IDs are unique")

# 2. REQUIRED FIELD CHECKS
print("\n2. REQUIRED FIELD VALIDATION")
print("-"*100)
required_fields = {
    'CUSTOMER_ID': 'External_Id__c',
    'TRADING_NAME': 'Name'
}

for oracle_col, sf_field in required_fields.items():
    null_count = df[oracle_col].isna().sum()
    if null_count > 0:
        print(f"   ⚠ CRITICAL: {sf_field} has {null_count:,} NULL values")
        issues.append(f"CRITICAL: {sf_field} has {null_count:,} NULL values")
    else:
        print(f"   ✓ {sf_field} - no NULLs")

# 3. DATA TYPE VALIDATION
print("\n3. DATA TYPE VALIDATION")
print("-"*100)

# Text field length checks (Salesforce limits)
text_limits = {
    'CUSTOMER_ID': ('External_Id__c', 18),
    'ABN': ('ABN__c', 11),
    'ACN': ('ACN__c', 9),
    'TRADING_NAME': ('Name', 255),
    'REG_STREET': ('BillingStreet', 255),
    'REG_SUBURB': ('BillingCity', 40),
    'REG_STATE': ('BillingState', 80),
    'REG_POSTCODE': ('BillingPostalCode', 20),
    'REG_COUNTRY': ('BillingCountry', 80),
    'POSTAL_STREET': ('ShippingStreet', 255),
    'POSTAL_SUBURB': ('ShippingCity', 40),
    'POSTAL_STATE': ('ShippingState', 80),
    'POSTAL_POSTCODE': ('ShippingPostalCode', 20),
    'POSTAL_COUNTRY': ('ShippingCountry', 80),
    'EMAIL_ADDRESS': ('BusinessEmail__c', 80),
}

for oracle_col, (sf_field, max_len) in text_limits.items():
    if oracle_col in df.columns:
        df[oracle_col] = df[oracle_col].astype(str).replace('nan', None)
        too_long = df[df[oracle_col].notna()][df[oracle_col].str.len() > max_len]
        if len(too_long) > 0:
            max_actual = df[df[oracle_col].notna()][oracle_col].str.len().max()
            print(f"   ⚠ WARNING: {sf_field} has {len(too_long):,} values exceeding {max_len} chars (max: {max_actual})")
            issues.append(f"WARNING: {sf_field} has {len(too_long):,} values > {max_len} chars")
        else:
            populated = df[oracle_col].notna().sum()
            print(f"   ✓ {sf_field} - all {populated:,} values within {max_len} char limit")

# Integer field validation
print("\n   Integer Fields:")
if 'NUMBER_OF_EMPLOYEES' in df.columns:
    try:
        invalid = df[df['NUMBER_OF_EMPLOYEES'].notna()][pd.to_numeric(df['NUMBER_OF_EMPLOYEES'], errors='coerce').isna()]
        if len(invalid) > 0:
            print(f"   ⚠ WARNING: NumberOfEmployees has {len(invalid):,} non-numeric values")
            issues.append(f"WARNING: NumberOfEmployees has {len(invalid):,} non-numeric values")
        else:
            print(f"   ✓ NumberOfEmployees - all numeric")
    except:
        print(f"   ⚠ WARNING: NumberOfEmployees validation failed")

# Date field validation
print("\n   Date Fields:")
if 'EMPLOYMENT_START_DATE' in df.columns:
    null_dates = df['EMPLOYMENT_START_DATE'].isna().sum()
    valid_dates = df['EMPLOYMENT_START_DATE'].notna().sum()
    print(f"   ✓ DateEmploymentCommenced__c - {valid_dates:,} valid dates, {null_dates:,} NULL")

# Boolean field validation
print("\n   Boolean Fields:")

# IS_POSTAL_DIFFERENT uses 1/0
if 'IS_POSTAL_DIFFERENT' in df.columns:
    invalid = df[df['IS_POSTAL_DIFFERENT'].notna()][~df['IS_POSTAL_DIFFERENT'].isin([0, 1])]
    if len(invalid) > 0:
        print(f"   ⚠ WARNING: IsPostalAddressDifferent__c has {len(invalid):,} invalid values (not 0/1)")
        print(f"      Invalid values: {invalid['IS_POSTAL_DIFFERENT'].unique()}")
        issues.append(f"WARNING: IsPostalAddressDifferent__c has {len(invalid):,} invalid boolean values")
    else:
        print(f"   ✓ IsPostalAddressDifferent__c - all values are 0/1")

# OWNER_PERFORM_TRADEWORK uses Y/N
if 'OWNER_PERFORM_TRADEWORK' in df.columns:
    invalid = df[df['OWNER_PERFORM_TRADEWORK'].notna()][~df['OWNER_PERFORM_TRADEWORK'].isin(['Y', 'N', 'y', 'n'])]
    if len(invalid) > 0:
        print(f"   ⚠ WARNING: OwnersPerformCoveredWork__c has {len(invalid):,} invalid values (not Y/N)")
        print(f"      Invalid values: {invalid['OWNER_PERFORM_TRADEWORK'].unique()}")
        issues.append(f"WARNING: OwnersPerformCoveredWork__c has {len(invalid):,} invalid boolean values")
    else:
        print(f"   ✓ OwnersPerformCoveredWork__c - all values are Y/N or NULL")

# 4. PICKLIST VALIDATION
print("\n4. PICKLIST VALUE VALIDATION")
print("-"*100)

if 'EMPLOYER_TYPE_CODE' in df.columns:
    unique_codes = df['EMPLOYER_TYPE_CODE'].unique()
    print(f"   Type (EMPLOYER_TYPE_CODE) values:")
    for code in sorted([c for c in unique_codes if pd.notna(c)]):
        count = (df['EMPLOYER_TYPE_CODE'] == code).sum()
        desc = employer_type_mapping.get(code, "UNMAPPED")
        if desc == "UNMAPPED":
            print(f"      ⚠ {code} → {desc} ({count:,} records)")
            issues.append(f"WARNING: EMPLOYER_TYPE_CODE '{code}' has no mapping")
        else:
            print(f"      ✓ {code} → {desc} ({count:,} records)")
    
    null_count = df['EMPLOYER_TYPE_CODE'].isna().sum()
    if null_count > 0:
        print(f"      ⚠ NULL values: {null_count:,}")

# 5. DATA POPULATION SUMMARY
print("\n5. DATA POPULATION SUMMARY")
print("-"*100)
print(f"{'Field':<40} {'Populated':<15} {'%':<10} {'NULL':<15}")
print("-"*100)

# Exclude the row number column
display_cols = [c for c in df.columns if c != 'RN']

for col in display_cols:
    populated = df[col].notna().sum()
    null_count = df[col].isna().sum()
    pct = (populated / len(df) * 100) if len(df) > 0 else 0
    print(f"{col:<40} {populated:>10,} {pct:>8.2f}% {null_count:>10,}")

# 6. FINAL SUMMARY
print("\n" + "="*100)
print("VALIDATION SUMMARY")
print("="*100)

if len(issues) == 0:
    print("✓ ALL CHECKS PASSED - Data is ready for load")
else:
    print(f"⚠ Found {len(issues)} issues:")
    for i, issue in enumerate(issues, 1):
        print(f"   {i}. {issue}")
    
    critical_issues = [i for i in issues if 'CRITICAL' in i]
    if critical_issues:
        print(f"\n⚠ CRITICAL: {len(critical_issues)} blocking issues must be resolved before load")
    else:
        print(f"\n✓ No critical issues - warnings can be reviewed but won't block load")

print("="*100)
