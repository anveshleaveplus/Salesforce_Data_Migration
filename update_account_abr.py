"""
Update existing Salesforce Accounts with SQL Server ABR enrichment data
Matches on ABN field and updates: ABNRegistrationDate__c, AccountStatus__c, 
Classifications__c, OSCACode__c
"""

import os
from dotenv import load_dotenv
import oracledb
import pyodbc
import pandas as pd
from simple_salesforce import Salesforce
from datetime import datetime

load_dotenv('.env.sit')

print("\n" + "="*80)
print("SIT ACCOUNT UPDATE - SQL Server ABR Enrichment")
print("="*80)
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Configuration
BATCH_SIZE = 500

# ============================================================================
# 1. Connect to Salesforce and get existing accounts with ABN
# ============================================================================
print("\n[1/6] Fetching existing Salesforce Accounts...")

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

# Query accounts with ABN
query = """
SELECT Id, External_Id__c, ABN__c, 
       ABNRegistrationDate__c, AccountStatus__c, 
       Classifications__c, OSCACode__c
FROM Account
WHERE External_Id__c != null AND ABN__c != null
"""

print("  Querying Salesforce...")
result = sf.query_all(query)
df_sf = pd.DataFrame(result['records'])
df_sf = df_sf.drop(columns=['attributes'], errors='ignore')

print(f"  [OK] Found {len(df_sf):,} accounts with ABN")

# ============================================================================
# 2. Extract ABR data from SQL Server
# ============================================================================
print("\n[2/6] Extracting ABR data from SQL Server...")

try:
    sql_server = 'cosql-test.coinvest.com.au'
    database = 'AvatarWarehouse'
    
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={sql_server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    
    print(f"  Connecting to {sql_server}...")
    sql_conn = pyodbc.connect(conn_str)
    
    # Query ABR data
    sql_query = """
    SELECT 
        CAST([Australian Business Number] AS VARCHAR) as ABN,
        [ABN Registration - Date of Effect] as ABN_Registration_Date,
        [ABN Status] as ABN_Status,
        [Main - Industry Class] as Industry_Class,
        [Main - Industry Class Code] as Industry_Class_Code
    FROM [datascience].[abr_cleaned]
    WHERE [Australian Business Number] IS NOT NULL
    """
    
    print("  Extracting ABR data...")
    
    # Use cursor instead of pandas to avoid warning
    cursor = sql_conn.cursor()
    cursor.execute(sql_query)
    
    abr_records = []
    for row in cursor.fetchall():
        abr_records.append({
            'ABN': row[0],
            'ABN_Registration_Date': row[1],
            'ABN_Status': row[2],
            'Industry_Class': row[3],
            'Industry_Class_Code': row[4]
        })
    
    df_abr = pd.DataFrame(abr_records)
    cursor.close()
    sql_conn.close()
    
    print(f"  [OK] Extracted {len(df_abr):,} ABR records")
    
except Exception as e:
    print(f"  [ERROR] SQL Server extraction failed: {e}")
    exit(1)

# ============================================================================
# 3. Join Salesforce accounts with SQL Server ABR data
# ============================================================================
print("\n[3/6] Matching Salesforce accounts with ABR data...")

# Clean ABN fields for joining
df_sf['ABN_CLEAN'] = df_sf['ABN__c'].astype(str).str.replace(' ', '').str.strip()
df_abr['ABN_CLEAN'] = df_abr['ABN'].astype(str).str.replace(' ', '').str.strip()

print(f"  SF ABN sample: {df_sf['ABN_CLEAN'].head(3).tolist()}")
print(f"  ABR ABN sample: {df_abr['ABN_CLEAN'].head(3).tolist()}")

# Join on cleaned ABN
df_merged = df_sf.merge(
    df_abr, 
    on='ABN_CLEAN', 
    how='left',
    suffixes=('', '_abr')
)

matched = df_merged['ABN_Status'].notna().sum()
print(f"  [OK] Matched {matched:,} of {len(df_sf):,} accounts ({matched/len(df_sf)*100:.1f}%)")

# ============================================================================
# 4. Map SQL Server values to Salesforce fields
# ============================================================================
print("\n[4/6] Preparing updates...")

# Map ABN Status: Active → Registered, Cancelled → Cancelled
status_mapping = {
    'Active': 'Registered',
    'Cancelled': 'Cancelled',
    'Suspended': 'Suspended'
}

df_merged['ABN_Status'] = df_merged['ABN_Status'].map(status_mapping)

# Convert date to ISO format
df_merged['ABN_Registration_Date'] = pd.to_datetime(
    df_merged['ABN_Registration_Date'], 
    errors='coerce'
).dt.strftime('%Y-%m-%d')

# Convert Industry Code to string
df_merged['Industry_Class_Code'] = df_merged['Industry_Class_Code'].apply(
    lambda x: str(int(x)) if pd.notna(x) else None
)

# Replace NaN with None
df_merged = df_merged.where(pd.notna(df_merged), None)

# Filter to only records that have at least one new value
# SKIP Classifications__c for now - picklist values don't match ANZSIC codes
df_to_update = df_merged[
    df_merged['ABN_Status'].notna() | 
    df_merged['ABN_Registration_Date'].notna() |
    df_merged['Industry_Class_Code'].notna()
].copy()

print(f"  Records to update: {len(df_to_update):,}")
print(f"  NOTE: Skipping Classifications__c (ANZSIC values don't match SF picklist)")
print(f"  Sample updates:")
for i, row in df_to_update.head(3).iterrows():
    print(f"    ID {row['Id']}: Status={row['ABN_Status']}, RegDate={row['ABN_Registration_Date']}, Code={row['Industry_Class_Code']}")

# ============================================================================
# 5. Update Salesforce accounts
# ============================================================================
print(f"\n[5/6] Updating {len(df_to_update):,} accounts in Salesforce...")
print(f"  Batch size: {BATCH_SIZE}")

success_count = 0
error_count = 0
errors = []

for i in range(0, len(df_to_update), BATCH_SIZE):
    batch = df_to_update.iloc[i:i+BATCH_SIZE]
    batch_num = i // BATCH_SIZE + 1
    total_batches = (len(df_to_update) + BATCH_SIZE - 1) // BATCH_SIZE
    
    # Prepare records for bulk update
    records = []
    for _, row in batch.iterrows():
        record = {
            'Id': row['Id'],
            'ABNRegistrationDate__c': row['ABN_Registration_Date'],
            'AccountStatus__c': row['ABN_Status'],
            # Skip Classifications__c - ANZSIC values don't match SF picklist
            # 'Classifications__c': row['Industry_Class'],
            'OSCACode__c': row['Industry_Class_Code']
        }
        records.append(record)
    
    try:
        results = sf.bulk.Account.update(records)
        
        # Count successes and errors
        batch_success = sum(1 for r in results if r['success'])
        batch_errors = [r for r in results if not r['success']]
        
        success_count += batch_success
        error_count += len(batch_errors)
        
        if batch_errors:
            errors.extend(batch_errors[:5])  # Keep first 5 errors
        
        print(f"  Batch {batch_num}/{total_batches}: {batch_success}/{len(batch)} successful")
        
    except Exception as e:
        print(f"  Batch {batch_num}/{total_batches}: [ERROR] {e}")
        error_count += len(batch)

# ============================================================================
# 6. Summary
# ============================================================================
print("\n" + "="*80)
print("UPDATE SUMMARY")
print("="*80)
print(f"Total accounts queried: {len(df_sf):,}")
print(f"Matched with ABR data: {matched:,} ({matched/len(df_sf)*100:.1f}%)")
print(f"Records updated: {len(df_to_update):,}")
print(f"Successful updates: {success_count:,}")
print(f"Failed updates: {error_count:,}")
print(f"Success rate: {success_count/len(df_to_update)*100:.1f}%")

if errors:
    print(f"\nSample errors:")
    for err in errors[:3]:
        print(f"  - {err}")

print("="*80)
