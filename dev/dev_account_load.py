"""
Local DEV Test - Oracle to Salesforce Account Sync
Run this locally to test the full workflow without Fabric/Azure
"""

import os
from dotenv import load_dotenv
import oracledb
import pandas as pd
from simple_salesforce import Salesforce
from datetime import datetime

# Load environment variables
load_dotenv()

# Configuration
LIMIT_ROWS = 200  # Test sample to check picklist values
BATCH_SIZE = 200

print("="*70)
print("DEV TEST - Oracle to Salesforce Account Sync")
print("="*70)
print(f"Test size: {LIMIT_ROWS:,} rows")
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

# Step 2b: Extract Account data
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
        CAST(NULL AS VARCHAR2(40)) as BUSINESS_PHONE,
        ws.EMPLOYMENT_START_DATE,
        e.WSR_TYPE_CODE,
        e.EMPLOYER_TYPE_CODE,
        es.EMPLOYER_REASON_CODE,
        es.EMPLOYER_STATUS_CODE,
        ROW_NUMBER() OVER (
            PARTITION BY e.CUSTOMER_ID 
            ORDER BY ws.EMPLOYMENT_START_DATE DESC NULLS LAST
        ) as rn
    FROM SCH_CO_20.CO_EMPLOYER e
    LEFT JOIN SCH_CO_20.CO_WSR_SERVICE ws 
        ON ws.WSR_ID = e.CUSTOMER_ID
    LEFT JOIN SCH_CO_20.CO_EMPLOYER_STATUS es
        ON es.EMPLOYER_STATUS_ID = e.CUSTOMER_ID
    WHERE 1=1
) WHERE rn = 1 AND ROWNUM <= {LIMIT_ROWS}
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
# 3. TRANSFORM AND MAP COLUMNS
# ============================================================================
print("\nStep 3: Transforming and mapping columns...")

# Column mapping: Oracle -> Salesforce
COLUMN_MAPPING = {
    'CUSTOMER_ID': 'External_Id__c',
    'CUSTOMER_ID_REG': 'Registration_Number__c',
    'ABN': 'ABN__c',
    'ACN': 'ACN__c',
    'TRADING_NAME': 'Name',
    'TRADING_NAME_REGISTERED': 'RegisteredEntityName__c',
    'TRADING_NAME_AS': 'TradingAs__c',
    'BUSINESS_PHONE': 'Phone',
    'EMPLOYMENT_START_DATE': 'DateEmploymentCommenced__c',
    'WSR_TYPE_CODE': 'AccountSubStatus__c',
    'EMPLOYER_TYPE_CODE': 'BusinessEntityType__c',
    'EMPLOYER_REASON_CODE': 'CoverageDeterminationStatus__c',
    'EMPLOYER_STATUS_CODE': 'Registration_Status__c'
}

# Apply transformations
df_mapped = df_oracle.copy()

# Map picklist codes to descriptions
for oracle_field, sf_field in [
    ('WSR_TYPE_CODE', 'AccountSubStatus__c'),
    ('EMPLOYER_TYPE_CODE', 'BusinessEntityType__c'),
    ('EMPLOYER_REASON_CODE', 'CoverageDeterminationStatus__c'),
    ('EMPLOYER_STATUS_CODE', 'Registration_Status__c')
]:
    if oracle_field in df_mapped.columns:
        df_mapped[sf_field] = df_mapped[oracle_field].map(code_mappings.get(oracle_field, {}))
        # Replace NaN with None for JSON compatibility
        df_mapped[sf_field] = df_mapped[sf_field].where(pd.notna(df_mapped[sf_field]), None)
        non_null = df_mapped[sf_field].notna().sum()
        print(f"  Mapped {non_null:,} {sf_field} values from {oracle_field}")

# Rename remaining columns
rename_cols = {k: v for k, v in COLUMN_MAPPING.items() if k in df_mapped.columns and v not in df_mapped.columns}
df_mapped = df_mapped.rename(columns=rename_cols)

# Drop original code columns (already mapped to descriptions)
code_cols_to_drop = ['WSR_TYPE_CODE', 'EMPLOYER_TYPE_CODE', 'EMPLOYER_REASON_CODE', 'EMPLOYER_STATUS_CODE']
df_mapped = df_mapped.drop(columns=[c for c in code_cols_to_drop if c in df_mapped.columns], errors='ignore')

# Trim string fields
for col in df_mapped.columns:
    if df_mapped[col].dtype == 'object':
        df_mapped[col] = df_mapped[col].str.strip()

# Convert dates to ISO format strings (JSON serializable)
if 'DateEmploymentCommenced__c' in df_mapped.columns:
    df_mapped['DateEmploymentCommenced__c'] = pd.to_datetime(
        df_mapped['DateEmploymentCommenced__c'], 
        errors='coerce'
    )
    # Convert to ISO format string, replace NaT with None
    df_mapped['DateEmploymentCommenced__c'] = df_mapped['DateEmploymentCommenced__c'].apply(
        lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None
    )

# Convert numbers to strings (ABN, ACN are numbers in Oracle but text in SF)
# Remove .0 suffix to avoid "STRING_TOO_LONG" errors
for col in ['ABN__c', 'ACN__c', 'External_Id__c', 'Registration_Number__c']:
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
    print(f"  WARNING: {duplicates} duplicate External_Id__c found (keeping first)")
    df_mapped = df_mapped.drop_duplicates(subset=['External_Id__c'], keep='first')

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
    error_file = f"error/dev_account_errors_{timestamp}.csv"
    error_df.to_csv(error_file, index=False)
    print(f"\n  [OK] Errors saved to: {error_file}")
    print(f"  Sample errors:")
    for error in errors[:3]:
        print(f"    - ID {error['external_id']}: {error['error']}")

# ============================================================================
# 7. VERIFICATION
# ============================================================================
print("\n" + "="*70)
print("VERIFICATION")
print("="*70)

try:
    # Count Salesforce records
    sf_count = sf.query(f"SELECT COUNT() FROM Account WHERE External_Id__c != null")['totalSize']
    print(f"[OK] Total Salesforce Accounts with External_Id__c: {sf_count:,}")
    
    # Sample 5 records
    sample = sf.query(f"SELECT External_Id__c, Registration_Number__c, Name FROM Account WHERE External_Id__c != null LIMIT 5")
    print(f"\nSample records:")
    for record in sample['records']:
        print(f"  - {record['External_Id__c']}: {record.get('Name', 'N/A')}")
    
except Exception as e:
    print(f"[ERROR] Verification failed: {e}")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)
