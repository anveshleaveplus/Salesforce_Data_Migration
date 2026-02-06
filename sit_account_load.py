"""
SIT - Oracle to Salesforce Account Sync (Active Employers)
Filters for employers with service records in 2023+
Includes reconciliation report and data quality checks
"""

import os
from dotenv import load_dotenv
import oracledb
import pyodbc
import pandas as pd
from simple_salesforce import Salesforce
from datetime import datetime

# Load environment variables
# For SIT, use .env.sit if it exists, otherwise use default .env
import sys
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)
print(f"Using environment: {env_file}\n")

# Configuration
LIMIT_ROWS = None  # Load all active employers (~54K)
BATCH_SIZE = 500
ACTIVE_PERIOD = 202301  # Filter: Service records >= Jan 2023

print("="*70)
print("SIT - Oracle to Salesforce Account Sync")
print("Active Employers Filter: Service >= 2023")
print("="*70)
print(f"Expected: ~54,000 active employers")
print(f"Batch size: {BATCH_SIZE}")
print()

# ============================================================================
# 1. CONNECT TO ORACLE
# ============================================================================
print("Step 1: Connecting to Oracle...")

try:
    connection = oracledb.connect(
        user=os.getenv('ORACLE_USER'),
        password=os.getenv('ORACLE_PASSWORD'),
        host=os.getenv('ORACLE_HOST'),
        port=int(os.getenv('ORACLE_PORT')),
        sid=os.getenv('ORACLE_SID')
    )
    print("[OK] Oracle connection successful")
except Exception as e:
    print(f"[ERROR] Oracle connection failed: {e}")
    exit(1)

# ============================================================================
# 2. EXTRACT DATA FROM ORACLE
# ============================================================================
print("\nStep 2: Extracting data from Oracle...")

# Step 2a: Load code mappings for picklist fields
print("  Loading code mappings...")
code_mappings = {}

code_fields = {
    'wsrtypecode': 'WSR_TYPE_CODE',
    'employertypecode': 'EMPLOYER_TYPE_CODE',
    'employerreasoncode': 'EMPLOYER_REASON_CODE',
    'employerstatuscode': 'EMPLOYER_STATUS_CODE'
}

cursor = connection.cursor()
for code_name, field_name in code_fields.items():
    try:
        # Get code_set_id
        cursor.execute(f"""
            SELECT code_set_id 
            FROM SCH_CO_20.CO_CODE_SET 
            WHERE LOWER(code_set_name) LIKE '{code_name}%'
        """)
        result = cursor.fetchone()
        
        if result:
            code_set_id = result[0]
            # Get value -> description mapping
            cursor.execute(f"""
                SELECT value, description 
                FROM SCH_CO_20.CO_CODE 
                WHERE code_set_id = {code_set_id}
            """)
            code_mappings[field_name] = {row[0]: row[1] for row in cursor.fetchall()}
            print(f"    Loaded {len(code_mappings[field_name])} values for {field_name}")
            # Print sample mappings
            sample = list(code_mappings[field_name].items())[:3]
            for val, desc in sample:
                print(f"      {val} -> {desc}")
        else:
            print(f"    WARNING: Code set not found for {code_name}")
            code_mappings[field_name] = {}
    except Exception as e:
        print(f"    ERROR loading {field_name}: {e}")
        code_mappings[field_name] = {}

print(f"  [OK] Loaded {len(code_mappings)} code mapping tables")

# Step 2b: Extract Account data - ACTIVE EMPLOYERS ONLY
print(f"\nFiltering for active employers (service records >= {ACTIVE_PERIOD})...")

# Build query with active employer filter
if LIMIT_ROWS:
    rownum_filter = f"AND ROWNUM <= {LIMIT_ROWS}"
else:
    rownum_filter = ""

oracle_query = f"""
SELECT * FROM (
    SELECT 
        e.CUSTOMER_ID,
        e.CUSTOMER_ID as CUSTOMER_ID_REG,
        e.ABN,
        e.ACN,
        e.TRADING_NAME,
        e.TRADING_NAME as TRADING_NAME_REGISTERED,
        e.TRADING_NAME as TRADING_NAME_AS,
        ws.EMPLOYMENT_START_DATE,
        e.WSR_TYPE_CODE,
        e.EMPLOYER_TYPE_CODE,
        es.EMPLOYER_REASON_CODE,
        es.EMPLOYER_STATUS_CODE,
        TRIM(
            COALESCE(addr.STREET, '') || 
            CASE WHEN addr.STREET2 IS NOT NULL THEN ' ' || addr.STREET2 ELSE '' END
        ) as BILLING_STREET,
        addr.SUBURB as BILLING_CITY,
        addr.STATE as BILLING_STATE,
        addr.POSTCODE as BILLING_POSTCODE,
        addr.COUNTRY_CODE as BILLING_COUNTRY,
        CASE 
            WHEN c.POSTAL_ADDRESS_ID IS NOT NULL 
                AND c.ADDRESS_ID != c.POSTAL_ADDRESS_ID 
            THEN 1 
            ELSE 0 
        END as IS_POSTAL_DIFFERENT,
        TRIM(
            COALESCE(postal_addr.STREET, '') || 
            CASE WHEN postal_addr.STREET2 IS NOT NULL THEN ' ' || postal_addr.STREET2 ELSE '' END
        ) as POSTAL_STREET,
        postal_addr.SUBURB as POSTAL_CITY,
        postal_addr.STATE as POSTAL_STATE,
        postal_addr.POSTCODE as POSTAL_POSTCODE,
        postal_addr.COUNTRY_CODE as POSTAL_COUNTRY,
        c.EMAIL_ADDRESS,
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
    LEFT JOIN SCH_CO_20.CO_EMPLOYER_STATUS es
        ON es.EMPLOYER_STATUS_ID = e.CUSTOMER_ID
    WHERE e.CUSTOMER_ID IN (
        SELECT DISTINCT ep.EMPLOYER_ID
        FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
        WHERE EXISTS (
            SELECT 1 FROM SCH_CO_20.CO_SERVICE s
            WHERE s.WORKER = ep.WORKER_ID
            AND s.PERIOD_END >= {ACTIVE_PERIOD}
        )
    )
) WHERE rn = 1 {rownum_filter}
"""

try:
    df_oracle = pd.read_sql(oracle_query, connection)
    connection.close()
    
    # Remove ROW_NUMBER column
    if 'RN' in df_oracle.columns:
        df_oracle = df_oracle.drop(columns=['RN'])
    
    print(f"[OK] Extracted {len(df_oracle):,} rows from Oracle")
    print(f"  Columns: {list(df_oracle.columns)}")
except Exception as e:
    print(f"[ERROR] Data extraction failed: {e}")
    connection.close()
    exit(1)

# ============================================================================
# 2.5. EXTRACT ABR DATA FROM SQL SERVER
# ============================================================================
print("\nStep 2.5: Extracting ABR data from SQL Server...")

try:
    # Connect to SQL Server using Windows Authentication
    sql_server = 'cosql-test.coinvest.com.au'
    database = 'AvatarWarehouse'
    
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={sql_server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    
    print(f"  Connecting to SQL Server: {sql_server}...")
    sql_conn = pyodbc.connect(conn_str)
    print("  [OK] SQL Server connection successful")
    
    # Extract ABR data - join on ABN (skip Industry_Class - values don't match SF picklist)
    sql_query = """
    SELECT 
        CAST([Australian Business Number] AS VARCHAR) as ABN,
        [ABN Registration - Date of Effect] as ABN_Registration_Date,
        [ABN Status] as ABN_Status,
        [Main - Industry Class Code] as Industry_Class_Code
    FROM [datascience].[abr_cleaned]
    WHERE [Australian Business Number] IS NOT NULL
    """
    
    print("  Extracting ABR data...")
    cursor = sql_conn.cursor()
    cursor.execute(sql_query)
    
    # Fetch all records into list of dicts
    abr_records = []
    for row in cursor.fetchall():
        abr_records.append({
            'ABN': row[0],
            'ABN_Registration_Date': row[1],
            'ABN_Status': row[2],
            'Industry_Class_Code': row[3]
        })
    
    df_abr = pd.DataFrame(abr_records)
    cursor.close()
    sql_conn.close()
    
    print(f"  [OK] Extracted {len(df_abr):,} ABR records")
    print(f"  ABR columns: {list(df_abr.columns)}")
    
    # Clean ABN for joining (remove spaces, ensure string format)
    df_abr['ABN'] = df_abr['ABN'].astype(str).str.replace(' ', '').str.strip()
    
    # Oracle ABN is NUMBER(11) - convert to string, handle NaN
    df_oracle['ABN_CLEAN'] = df_oracle['ABN'].apply(
        lambda x: str(int(x)) if pd.notna(x) and x != '' else None
    )
    
    # Left join Oracle data with SQL Server ABR data
    df_oracle = df_oracle.merge(df_abr, left_on='ABN_CLEAN', right_on='ABN', how='left', suffixes=('', '_ABR'))
    
    # Map SQL Server ABN Status values to Salesforce picklist values
    # SQL: "Active" → SF: "Registered"
    # SQL: "Cancelled" → SF: "Cancelled" (same)
    status_mapping = {
        'Active': 'Registered',
        'Cancelled': 'Cancelled'
    }
    df_oracle['ABN_Status'] = df_oracle['ABN_Status'].map(status_mapping)
    
    # Count successful matches
    matched = df_oracle['ABN_Registration_Date'].notna().sum()
    print(f"  [OK] Matched {matched:,} of {len(df_oracle):,} Oracle records ({matched/len(df_oracle)*100:.1f}%)")
    print(f"  [OK] Mapped ABN Status values: Active to Registered, Cancelled to Cancelled")
    
    # Drop temporary join column
    df_oracle = df_oracle.drop(columns=['ABN_CLEAN', 'ABN_ABR'], errors='ignore')
    
    # Replace NaN with None for SQL Server fields (to avoid JSON serialization errors)
    sql_fields = ['ABN_Registration_Date', 'ABN_Status', 'Industry_Class_Code']
    for field in sql_fields:
        if field in df_oracle.columns:
            df_oracle[field] = df_oracle[field].replace({pd.NA: None, pd.NaT: None})
            df_oracle[field] = df_oracle[field].where(pd.notna(df_oracle[field]), None)
    
except Exception as e:
    print(f"  [WARNING] SQL Server extraction failed: {e}")
    print("  Continuing without ABR data...")
    # Add empty columns so mapping doesn't fail
    df_oracle['ABN_Registration_Date'] = None
    df_oracle['ABN_Status'] = None
    df_oracle['Industry_Class_Code'] = None

# ============================================================================
# 3. TRANSFORM AND MAP COLUMNS
# ============================================================================
print("\nStep 3: Transforming and mapping columns...")

# Column mapping: Oracle -> Salesforce
# NOTE: Picklist fields skipped for SIT due to value mismatches with Oracle
COLUMN_MAPPING = {
    'CUSTOMER_ID': 'External_Id__c',
    'CUSTOMER_ID_REG': 'Registration_Number__c',
    'ABN': 'ABN__c',
    'ACN': 'ACN__c',
    'TRADING_NAME': 'Name',
    'TRADING_NAME_REGISTERED': 'RegisteredEntityName__c',
    'TRADING_NAME_AS': 'TradingAs__c',
    'EMPLOYMENT_START_DATE': 'DateEmploymentCommenced__c',
    'BILLING_STREET': 'BillingStreet',
    'BILLING_CITY': 'BillingCity',
    'BILLING_STATE': 'BillingState',
    'BILLING_POSTCODE': 'BillingPostalCode',
    'BILLING_COUNTRY': 'BillingCountry',
    'IS_POSTAL_DIFFERENT': 'IsPostalAddressDifferent__c',
    'POSTAL_STREET': 'ShippingStreet',
    'POSTAL_CITY': 'ShippingCity',
    'POSTAL_STATE': 'ShippingState',
    'POSTAL_POSTCODE': 'ShippingPostalCode',
    'POSTAL_COUNTRY': 'ShippingCountry',
    'EMAIL_ADDRESS': 'BusinessEmail__c',
    # SQL Server ABR fields:
    'ABN_Registration_Date': 'ABNRegistrationDate__c',
    'ABN_Status': 'AccountStatus__c',
    # Skip Classifications__c - ANZSIC values don't match SF picklist
    'Industry_Class_Code': 'OSCACode__c',
    # SKIPPED PICKLIST FIELDS (values don't match SIT):
    # 'WSR_TYPE_CODE': 'AccountSubStatus__c',
    # 'EMPLOYER_TYPE_CODE': 'BusinessEntityType__c',
    # 'EMPLOYER_REASON_CODE': 'CoverageDeterminationStatus__c',
    # 'EMPLOYER_STATUS_CODE': 'Registration_Status__c'
}

# Apply transformations
df_mapped = df_oracle.copy()

# Map picklist codes to descriptions (ALL SKIPPED for SIT due to value mismatches)
print("  NOTE: Skipping all picklist fields - values don't exist in SIT environment")
print("        Fields skipped: AccountSubStatus__c, BusinessEntityType__c,")
print("                        CoverageDeterminationStatus__c, Registration_Status__c")

# PICKLIST MAPPING DISABLED FOR SIT
# All picklist fields skipped - SIT has different picklist values than Oracle

# Rename remaining columns
rename_cols = {k: v for k, v in COLUMN_MAPPING.items() if k in df_mapped.columns and v not in df_mapped.columns}
df_mapped = df_mapped.rename(columns=rename_cols)

# Replace ALL NaN/NaT values with None for JSON serialization
print("  Replacing NaN/NaT values with None...")
df_mapped = df_mapped.where(pd.notna(df_mapped), None)
for col in df_mapped.columns:
    df_mapped[col] = df_mapped[col].replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})

# Drop original code columns (already mapped to descriptions)
code_cols_to_drop = ['WSR_TYPE_CODE', 'EMPLOYER_TYPE_CODE', 'EMPLOYER_REASON_CODE', 'EMPLOYER_STATUS_CODE']
df_mapped = df_mapped.drop(columns=[c for c in code_cols_to_drop if c in df_mapped.columns], errors='ignore')

# Trim string fields (only for actual string columns)
for col in df_mapped.columns:
    if df_mapped[col].dtype == 'object':
        # Check if column actually contains strings
        if df_mapped[col].apply(lambda x: isinstance(x, str)).any():
            df_mapped[col] = df_mapped[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

# Convert dates to ISO format strings (JSON serializable)
for date_field in ['DateEmploymentCommenced__c', 'ABNRegistrationDate__c']:
    if date_field in df_mapped.columns:
        df_mapped[date_field] = pd.to_datetime(
            df_mapped[date_field], 
            errors='coerce'
        )
        # Convert to ISO format string, replace NaT with None
        df_mapped[date_field] = df_mapped[date_field].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None
        )

# Convert numbers to strings (ABN, ACN are numbers in Oracle but text in SF)
# Remove .0 suffix to avoid "STRING_TOO_LONG" errors
for col in ['ABN__c', 'ACN__c', 'External_Id__c', 'Registration_Number__c', 'OSCACode__c']:
    if col in df_mapped.columns:
        df_mapped[col] = df_mapped[col].apply(
            lambda x: str(int(x)) if pd.notna(x) and x != '' else None
        )

print(f"[OK] Transformed {len(df_mapped):,} rows")
print(f"  Mapped columns: {list(df_mapped.columns)}")

# Data quality checks
null_ids = df_mapped['External_Id__c'].isnull().sum()
if null_ids > 0:
    print(f"  WARNING: {null_ids} records with NULL External_Id__c (will fail UPSERT)")
    df_mapped = df_mapped[df_mapped['External_Id__c'].notnull()]

duplicates = df_mapped['External_Id__c'].duplicated().sum()
if duplicates > 0:
    print(f"  WARNING: {duplicates} duplicate External_Id__c found")
    print(f"  Prioritizing rows with ABN data to maximize SQL Server enrichment...")
    # Sort by ABN descending (non-null values first) before deduplication
    # This ensures we keep the row WITH ABN data when duplicates exist
    df_mapped = df_mapped.sort_values('ABN__c', ascending=False, na_position='last')
    df_mapped = df_mapped.drop_duplicates(subset=['External_Id__c'], keep='first')
    print(f"  Kept records with ABN where possible")

print(f"  Final record count: {len(df_mapped):,}")

# ============================================================================
# 4. CONNECT TO SALESFORCE
# ============================================================================
print("\nStep 4: Connecting to Salesforce...")

try:
    sf = Salesforce(
        username=os.getenv('SF_USERNAME'),
        password=os.getenv('SF_PASSWORD'),
        security_token=os.getenv('SF_SECURITY_TOKEN'),
        domain=os.getenv('SF_DOMAIN')
    )
    print(f"[OK] Salesforce connection successful")
    print(f"  Instance: {sf.sf_instance}")
    print(f"  Username: {os.getenv('SF_USERNAME')}")
except Exception as e:
    print(f"[ERROR] Salesforce connection failed: {e}")
    exit(1)

# ============================================================================
# 4.5. VERIFY EXTERNAL ID FIELD SETUP
# ============================================================================
print("\nStep 4.5: Verifying External_Id__c field setup...")

try:
    account_metadata = sf.Account.describe()
    external_id_field = next((f for f in account_metadata['fields'] if f['name'] == 'External_Id__c'), None)
    
    if not external_id_field:
        print("\n[ERROR] External_Id__c field does not exist on Account object")
        print("\nPlease create the field first:")
        print("  1. Salesforce Setup -> Object Manager -> Account")
        print("  2. Fields & Relationships -> New")
        print("  3. Data Type: Text, Field Name: External_Id__c")
        print("  4. Check 'External ID' and 'Unique' checkboxes")
        exit(1)
    
    if not external_id_field.get('externalId'):
        print("\n[ERROR] External_Id__c exists but is NOT marked as External ID")
        print("\nTo fix this:")
        print("  1. Salesforce Setup -> Object Manager -> Account")
        print("  2. Fields & Relationships -> External_Id__c -> Edit")
        print("  3. Check 'External ID' checkbox -> Save")
        exit(1)
    
    print(f"[OK] External_Id__c is properly configured as External ID")
    print(f"  - Type: {external_id_field['type']}")
    print(f"  - Unique: {external_id_field.get('unique', False)}")
    print(f"  - Case Sensitive: {external_id_field.get('caseSensitive', False)}")
    
except Exception as e:
    print(f"\n[WARNING] Could not verify External ID field: {str(e)}")
    print("Proceeding anyway... (will fail at UPSERT if not configured)")

# ============================================================================
# 5. UPSERT TO SALESFORCE
# ============================================================================
print(f"\nStep 5: Upserting {len(df_mapped):,} records to Salesforce...")
print(f"  External ID field: External_Id__c")
print(f"  Batch size: {BATCH_SIZE}")

# Show sample picklist values being sent
print(f"\n  Sample picklist values (first 3 records):")
for idx in range(min(3, len(df_mapped))):
    record = df_mapped.iloc[idx]
    print(f"    Record {idx+1}:")
    print(f"      AccountSubStatus__c: {record.get('AccountSubStatus__c')}")
    print(f"      BusinessEntityType__c: {record.get('BusinessEntityType__c')}")
    print(f"      CoverageDeterminationStatus__c: {record.get('CoverageDeterminationStatus__c')}")
    print(f"      Registration_Status__c: {record.get('Registration_Status__c')}")
print()

start_time = datetime.now()
success_count = 0
error_count = 0
errors = []

# Process in batches
total_batches = (len(df_mapped) + BATCH_SIZE - 1) // BATCH_SIZE

for i in range(0, len(df_mapped), BATCH_SIZE):
    batch_num = (i // BATCH_SIZE) + 1
    batch = df_mapped.iloc[i:i+BATCH_SIZE]
    
    # Convert to list of dicts (remove None values for cleaner API calls)
    records = batch.to_dict('records')
    records = [{k: v for k, v in record.items() if v is not None} for record in records]
    
    print(f"  Batch {batch_num}/{total_batches} ({len(records)} records)...", end=" ")
    
    try:
        # UPSERT using External_Id__c (use serial processing to avoid timeout)
        result = sf.bulk.Account.upsert(records, 'External_Id__c', batch_size=BATCH_SIZE, use_serial=True)
        
        # Count successes and errors
        batch_success = sum(1 for r in result if r.get('success'))
        batch_errors = len(result) - batch_success
        
        success_count += batch_success
        error_count += batch_errors
        
        # Collect error details
        for idx, r in enumerate(result):
            if not r.get('success'):
                error_record = {
                    'batch': batch_num,
                    'index': i + idx,
                    'external_id': batch.iloc[idx]['External_Id__c'],
                    'error': r.get('errors', 'Unknown error')
                }
                errors.append(error_record)
        
        print(f"[OK] {batch_success} success, {batch_errors} errors")
        
    except Exception as e:
        error_count += len(records)
        print(f"[ERROR] Failed: {str(e)}")
        for idx in range(len(records)):
            error_record = {
                'batch': batch_num,
                'index': i + idx,
                'external_id': batch.iloc[idx]['External_Id__c'],
                'error': str(e)
            }
            errors.append(error_record)

# ============================================================================
# 6. SUMMARY
# ============================================================================
end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"Total processed:   {len(df_mapped):,} records")
print(f"Successful:        {success_count:,} records")
print(f"Failed:            {error_count:,} records")
print(f"Success rate:      {(success_count/len(df_mapped)*100):.1f}%")
print(f"Duration:          {duration:.1f} seconds ({duration/60:.1f} minutes)")
print(f"Throughput:        {len(df_mapped)/duration:.1f} records/second")
print()

# Save errors to CSV if any
if errors:
    error_df = pd.DataFrame(errors)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_file = f"error/sit_account_errors_{timestamp}.csv"
    error_df.to_csv(error_file, index=False)
    print(f"\n  [OK] Errors saved to: {error_file}")
    print(f"  Sample errors:")
    for error in errors[:3]:
        print(f"    - ID {error['external_id']}: {error['error']}")

# ============================================================================
# 7. RECONCILIATION & DATA QUALITY CHECKS
# ============================================================================
print("\n" + "="*70)
print("RECONCILIATION & DATA QUALITY CHECKS")
print("="*70)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    # 1. Count records in both systems
    print("\n[1/5] Record Counts")
    print("-" * 70)
    oracle_count = len(df_oracle)
    sf_count = sf.query(f"SELECT COUNT() FROM Account WHERE External_Id__c != null")['totalSize']
    print(f"  Oracle extracted:  {oracle_count:,} records")
    print(f"  Salesforce total:  {sf_count:,} records with External_Id__c")
    print(f"  Match status:      {'✓ MATCH' if oracle_count <= sf_count else '✗ MISMATCH'}")
    
    # 2. Data Quality - Missing required fields
    print("\n[2/5] Data Quality - Required Fields")
    print("-" * 70)
    dq_query = """
        SELECT COUNT() total,
               SUM(CASE WHEN Name = null THEN 1 ELSE 0 END) missing_name,
               SUM(CASE WHEN Registration_Number__c = null THEN 1 ELSE 0 END) missing_reg_number
        FROM Account 
        WHERE External_Id__c != null
    """
    # Note: SOQL doesn't support aggregate CASE, so query individually
    total_accounts = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null")['totalSize']
    missing_name = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null AND Name = null")['totalSize']
    missing_reg = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null AND Registration_Number__c = null")['totalSize']
    
    print(f"  Total Accounts:         {total_accounts:,}")
    print(f"  Missing Name:           {missing_name:,} ({missing_name/total_accounts*100:.2f}%)")
    print(f"  Missing Reg Number:     {missing_reg:,} ({missing_reg/total_accounts*100:.2f}%)")
    
    # 3. Picklist Value Coverage
    print("\n[3/5] Picklist Value Distribution")
    print("-" * 70)
    
    picklist_fields = [
        'Registration_Status__c',
        'BusinessEntityType__c',
        'CoverageDeterminationStatus__c',
        'AccountSubStatus__c'
    ]
    
    for field in picklist_fields:
        try:
            result = sf.query(f"SELECT {field}, COUNT(Id) cnt FROM Account WHERE External_Id__c != null GROUP BY {field}")
            print(f"  {field}:")
            for record in result['records'][:5]:  # Top 5 values
                value = record[field] or 'NULL'
                count = record['cnt']
                print(f"    - {value}: {count:,}")
        except Exception as e:
            print(f"  {field}: Error - {e}")
    
    # 4. Sample Records Verification
    print("\n[4/5] Sample Records Verification")
    print("-" * 70)
    sample = sf.query("""
        SELECT External_Id__c, Registration_Number__c, Name, 
               Registration_Status__c, BusinessEntityType__c
        FROM Account 
        WHERE External_Id__c != null 
        ORDER BY External_Id__c
        LIMIT 5
    """)
    for record in sample['records']:
        print(f"  External ID: {record['External_Id__c']}")
        print(f"    Name: {record.get('Name', 'NULL')}")
        print(f"    Reg#: {record.get('Registration_Number__c', 'NULL')}")
        print(f"    Status: {record.get('Registration_Status__c', 'NULL')}")
        print(f"    Type: {record.get('BusinessEntityType__c', 'NULL')}")
    
    # 5. Generate Reconciliation Report
    print("\n[5/5] Generating Reconciliation Report")
    print("-" * 70)
    
    recon_report = {
        'Timestamp': timestamp,
        'Oracle_Extracted': oracle_count,
        'Salesforce_Total': sf_count,
        'Records_Matched': oracle_count <= sf_count,
        'Success_Count': success_count,
        'Error_Count': error_count,
        'Success_Rate': f"{success_count/oracle_count*100:.2f}%" if oracle_count > 0 else "0%",
        'Missing_Name': missing_name,
        'Missing_Reg_Number': missing_reg,
        'Data_Quality_Score': f"{(total_accounts - missing_name - missing_reg)/total_accounts*100:.2f}%" if total_accounts > 0 else "0%"
    }
    
    recon_df = pd.DataFrame([recon_report])
    recon_file = f"test_output/sit_account_reconciliation_{timestamp}.csv"
    recon_df.to_csv(recon_file, index=False)
    print(f"  ✓ Reconciliation report saved to: {recon_file}")
    
    print("\n" + "="*70)
    print("RECONCILIATION SUMMARY")
    print("="*70)
    for key, value in recon_report.items():
        print(f"  {key:25}: {value}")
    
except Exception as e:
    print(f"[ERROR] Reconciliation failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)
