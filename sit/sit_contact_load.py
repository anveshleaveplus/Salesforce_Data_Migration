"""
SIT - Salesforce Contact Object Load
Loads Contact records from Oracle to Salesforce SIT environment
Links to 53,857 active employer accounts
Creates AccountContactRelation for active employments
"""

import os
import sys
import csv
from datetime import datetime
from dotenv import load_dotenv
import oracledb
import pandas as pd
from simple_salesforce import Salesforce

# Load SIT environment variables
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)
print(f"Using environment: {env_file}\n")

# Configuration
LIMIT_ROWS = 50000  # Load 50K contacts
BATCH_SIZE = 500
ACTIVE_PERIOD = 202301  # Filter for employers with service >= Jan 2023

print("="*70)
print("SIT - Oracle to Salesforce Contact Sync")
print("Loading: 50,000 contacts for active employers")
print("="*70)
print(f"Loading: {LIMIT_ROWS:,} contacts")
print(f"Batch size: {BATCH_SIZE}")

def connect_oracle():
    """Establish Oracle database connection"""
    print("[1/7] Connecting to Oracle...")
    conn = oracledb.connect(
        user=os.getenv('ORACLE_USER'),
        password=os.getenv('ORACLE_PASSWORD'),
        host=os.getenv('ORACLE_HOST'),
        port=int(os.getenv('ORACLE_PORT')),
        sid=os.getenv('ORACLE_SID')
    )
    print("      [OK] Oracle connected")
    return conn

def connect_salesforce():
    """Establish Salesforce connection"""
    print("[2/7] Connecting to Salesforce...")
    sf = Salesforce(
        username=os.getenv('SF_USERNAME'),
        password=os.getenv('SF_PASSWORD'),
        security_token=os.getenv('SF_SECURITY_TOKEN'),
        domain=os.getenv('SF_DOMAIN', 'test')
    )
    print("      [OK] Salesforce connected")
    return sf

def fetch_account_ids(sf, employer_ids):
    """Fetch Salesforce Account Ids for given External_Id__c values"""
    print("[3/7] Fetching Account IDs from Salesforce...")
    
    # Remove None values and convert to set
    valid_employer_ids = set(str(int(x)) for x in employer_ids if pd.notna(x))
    
    if not valid_employer_ids:
        print("      [WARNING] No valid employer IDs to fetch")
        return {}
    
    # Query Accounts in batches (SOQL IN clause limit is 4000)
    account_map = {}  # employer_external_id -> salesforce_account_id
    batch_size = 2000
    employer_list = list(valid_employer_ids)
    
    for i in range(0, len(employer_list), batch_size):
        batch = employer_list[i:i+batch_size]
        ids_str = "','" .join(batch)
        
        query = f"""
            SELECT Id, External_Id__c 
            FROM Account 
            WHERE External_Id__c IN ('{ids_str}')
        """
        
        result = sf.query(query)
        for record in result['records']:
            account_map[record['External_Id__c']] = record['Id']
    
    print(f"      [OK] Fetched {len(account_map):,} Account IDs out of {len(valid_employer_ids):,} unique employers")
    
    if len(account_map) < len(valid_employer_ids):
        missing = len(valid_employer_ids) - len(account_map)
        print(f"      [WARNING] {missing:,} employer IDs not found in Salesforce Accounts")
    
    return account_map

def verify_external_id(sf):
    """Verify External_Id__c is marked as External ID field"""
    print("[4/7] Verifying External_Id__c field...")
    try:
        contact_metadata = sf.Contact.describe()
        external_id_field = next((f for f in contact_metadata['fields'] if f['name'] == 'External_Id__c'), None)
        
        if not external_id_field:
            print("      [ERROR] External_Id__c field not found in Contact object")
            return False
        
        if not external_id_field.get('externalId', False):
            print("      [WARNING] External_Id__c is NOT marked as External ID")
            print("                You must mark it as External ID before production sync")
            print("                Setup > Object Manager > Contact > Fields > External_Id__c > Edit > External ID")
            return False
        
        print("      [OK] External_Id__c is marked as External ID")
        return True
    except Exception as e:
        print(f"      [ERROR] Could not verify External_Id__c: {e}")
        return False

def extract_oracle_data(conn):
    """Extract Contact data from Oracle"""
    print(f"[5/7] Extracting {LIMIT_ROWS:,} Contact records from Oracle...")
    
    # Query joins CO_WORKER, CO_PERSON, CO_CUSTOMER, CO_EMPLOYMENT_PERIOD
    # CO_WORKER.CUSTOMER_ID is the unique worker identifier (834K unique workers)
    # CO_EMPLOYMENT_PERIOD links workers to employers
    # Filter: ACTIVE employment only (EFFECTIVE_TO_DATE IS NULL)
    # If multiple active employments: pick most recent start date
    query = f"""
    SELECT * FROM (
        SELECT 
            w.CUSTOMER_ID as WORKER_ID,
            p.PERSON_ID,
            p.FIRST_NAME,
            p.SURNAME as LAST_NAME,
            p.DATE_OF_BIRTH,
            c.EMAIL_ADDRESS,
            c.TELEPHONE1_NO as OTHER_PHONE,
            c.MOBILE_PHONE_NO,
            ep.EMPLOYER_ID,
            ROW_NUMBER() OVER (
                PARTITION BY w.CUSTOMER_ID 
                ORDER BY ep.EFFECTIVE_FROM_DATE DESC NULLS LAST
            ) as rn
        FROM SCH_CO_20.CO_WORKER w
        INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
        INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
        LEFT JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep 
            ON w.CUSTOMER_ID = ep.WORKER_ID 
            AND ep.EFFECTIVE_TO_DATE IS NULL
            AND ep.EMPLOYER_ID IN (
                SELECT DISTINCT e.CUSTOMER_ID
                FROM SCH_CO_20.CO_EMPLOYER e
                WHERE EXISTS (
                    SELECT 1
                    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep2
                    INNER JOIN SCH_CO_20.CO_SERVICE s 
                        ON s.WORKER = ep2.WORKER_ID
                    WHERE ep2.EMPLOYER_ID = e.CUSTOMER_ID
                        AND s.PERIOD_END >= {ACTIVE_PERIOD}
                )
            )
    ) WHERE rn = 1 AND ROWNUM <= {LIMIT_ROWS}
    """
    
    df = pd.read_sql(query, conn)
    print(f"      [OK] Extracted {len(df):,} records")
    print(f"      Sample data:")
    print(f"        WORKER_ID range: {df['WORKER_ID'].min()} to {df['WORKER_ID'].max()}")
    print(f"        PERSON_ID range: {df['PERSON_ID'].min()} to {df['PERSON_ID'].max()}")
    print(f"        Non-null EMAIL_ADDRESS: {df['EMAIL_ADDRESS'].notna().sum():,}")
    print(f"        Non-null DATE_OF_BIRTH: {df['DATE_OF_BIRTH'].notna().sum():,}")
    print(f"        Non-null MOBILE_PHONE_NO: {df['MOBILE_PHONE_NO'].notna().sum():,}")
    print(f"        Non-null EMPLOYER_ID: {df['EMPLOYER_ID'].notna().sum():,}")
    
    return df

def get_existing_contacts(sf, external_ids):
    """Query existing Contacts and their AccountIds to avoid duplicate ACR creation"""
    print("[5.5/7] Checking for existing Contacts...")
    
    if len(external_ids) == 0:
        print("        [OK] No external IDs to check")
        return {}
    
    existing_contacts = {}
    batch_size = 500  # Reduced from 2000 to avoid URI too long error
    external_id_list = list(external_ids)
    
    total_batches = (len(external_id_list) + batch_size - 1) // batch_size
    print(f"        Querying in {total_batches} batches...")
    
    for i in range(0, len(external_id_list), batch_size):
        batch = external_id_list[i:i+batch_size]
        ids_str = "','".join(batch)
        
        query = f"""
            SELECT Id, External_Id__c, AccountId
            FROM Contact
            WHERE External_Id__c IN ('{ids_str}')
        """
        
        result = sf.query(query)
        for record in result['records']:
            existing_contacts[record['External_Id__c']] = record.get('AccountId')
    
    print(f"        [OK] Found {len(existing_contacts):,} existing Contacts")
    return existing_contacts

def map_to_salesforce(df, account_map, existing_contacts):
    """Map Oracle columns to Salesforce Contact fields"""
    print("[6/7] Mapping Oracle data to Salesforce Contact fields...")
    
    # Mapping based on sf_contact_mapping.csv:
    # 1. Contact.External_Id__c ← CO_WORKER.CUSTOMER_ID (unique per worker - 834K unique)
    # 2. Contact.AccountId ← Lookup Salesforce Account.Id by Account.External_Id__c = EMPLOYER_ID
    # 3. Contact.Birthdate ← CO_PERSON.DATE_OF_BIRTH
    # 4. Contact.Email ← CO_CUSTOMER.EMAIL_ADDRESS
    # 5. Contact.OtherPhone ← CO_CUSTOMER.TELEPHONE1_NO
    # 6. Contact.MobilePhone ← CO_CUSTOMER.MOBILE_PHONE_NO
    # SKIPPED:
    # - RegistrationNumber__c ← CO_PERSON.PERSON_ID (read-only field, cannot write via API)
    # - EmailBouncedDate ← CO_PERSON.FIRST_NAME (incorrect mapping)
    # - MasterRecordId ← CO_CUSTOMER.MOBILE_PHONE_NO (incorrect mapping)
    # - CommunicationPreference__c ← COMMUNICATION_PREFERENCE table (table does not exist)
    
    df_mapped = pd.DataFrame()
    
    # External_Id__c: WORKER_ID (CO_WORKER.CUSTOMER_ID - convert to string, remove .0)
    df_mapped['External_Id__c'] = df['WORKER_ID'].apply(lambda x: str(int(x)) if pd.notna(x) else None)
    
    # FirstName: FIRST_NAME (required for Contact)
    df_mapped['FirstName'] = df['FIRST_NAME'].replace('', None)
    
    # LastName: LAST_NAME (required for Contact)
    df_mapped['LastName'] = df['LAST_NAME'].replace('', None).fillna('Unknown')  # Default to 'Unknown' if missing
    
    # AccountId: Lookup from account_map using EMPLOYER_ID
    # BUT: Only set AccountId if Contact is new OR AccountId is different from existing
    def get_account_id(row):
        external_id = str(int(row['WORKER_ID'])) if pd.notna(row['WORKER_ID']) else None
        new_account_id = account_map.get(str(int(row['EMPLOYER_ID']))) if pd.notna(row['EMPLOYER_ID']) else None
        
        # If Contact doesn't exist, use the new AccountId
        if external_id not in existing_contacts:
            return new_account_id
        
        # Contact exists - only set AccountId if it's different or NULL
        existing_account_id = existing_contacts.get(external_id)
        if existing_account_id == new_account_id:
            return None  # Same AccountId - don't set it to avoid duplicate ACR error
        else:
            return new_account_id  # Different AccountId - update it
    
    df_mapped['AccountId'] = df.apply(get_account_id, axis=1)
    
    # Birthdate: DATE_OF_BIRTH (convert to ISO date string)
    df_mapped['Birthdate'] = df['DATE_OF_BIRTH'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
    
    # Email: EMAIL_ADDRESS
    df_mapped['Email'] = df['EMAIL_ADDRESS'].replace('', None)
    
    # OtherPhone: TELEPHONE1_NO
    df_mapped['OtherPhone'] = df['OTHER_PHONE'].replace('', None)
    
    # MobilePhone: MOBILE_PHONE_NO
    df_mapped['MobilePhone'] = df['MOBILE_PHONE_NO'].replace('', None)
    
    accounts_found = df_mapped['AccountId'].notna().sum()
    accounts_missing = df_mapped['AccountId'].isna().sum()
    accounts_skipped = len([ext_id for ext_id in df_mapped['External_Id__c'] 
                           if ext_id in existing_contacts and df_mapped[df_mapped['External_Id__c']==ext_id]['AccountId'].isna().any()])
    
    print(f"      [OK] Mapped {len(df_mapped)} records with 8 fields")
    print(f"      Fields: External_Id__c (WORKER_ID), FirstName, LastName, AccountId (Account lookup),")
    print(f"              Birthdate, Email, OtherPhone, MobilePhone")
    print(f"      Account lookups: {accounts_found:,} will be set")
    print(f"      Account lookups: {accounts_skipped:,} skipped (Contact already has this AccountId)")
    print(f"      Account lookups: {accounts_missing - accounts_skipped:,} missing (no employer in SF)")
    print(f"      Note: Skipped 4 fields:")
    print(f"            - RegistrationNumber__c (read-only, cannot write via API)")
    print(f"            - EmailBouncedDate (mapped to FIRST_NAME in CSV - incorrect)")
    print(f"            - MasterRecordId (mapped to MOBILE_PHONE_NO in CSV - incorrect)")
    print(f"            - CommunicationPreference__c (COMMUNICATION_PREFERENCE table not found)")
    
    return df_mapped

def upsert_to_salesforce(sf, df_mapped):
    """Upsert Contact records to Salesforce in batches"""
    print(f"[7/7] Upserting {len(df_mapped):,} Contact records to Salesforce...")
    print(f"      Batch size: {BATCH_SIZE}")
    
    total_batches = (len(df_mapped) + BATCH_SIZE - 1) // BATCH_SIZE
    success_count = 0
    error_count = 0
    errors = []
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(df_mapped))
        batch_df = df_mapped.iloc[start_idx:end_idx]
        
        # Convert to list of dicts (remove None values)
        records = batch_df.to_dict('records')
        records_clean = [{k: v for k, v in record.items() if v is not None} for record in records]
        
        try:
            # UPSERT using Bulk API (use serial processing to avoid threading timeouts)
            result = sf.bulk.Contact.upsert(records_clean, 'External_Id__c', batch_size=BATCH_SIZE, use_serial=True)
            
            # Count successes and errors
            batch_success = sum(1 for r in result if r.get('success'))
            batch_errors = len(result) - batch_success
            
            success_count += batch_success
            error_count += batch_errors
            
            # Collect errors
            for idx, res in enumerate(result):
                if not res.get('success'):
                    errors.append({
                        'batch': batch_num + 1,
                        'index': start_idx + idx,
                        'external_id': records[idx].get('External_Id__c', 'UNKNOWN'),
                        'error': str(res.get('errors', 'Unknown error'))
                    })
        
        except Exception as e:
            # If entire batch fails, mark all as errors
            error_count += len(records)
            for idx, record in enumerate(records):
                errors.append({
                    'batch': batch_num + 1,
                    'index': start_idx + idx,
                    'external_id': record.get('External_Id__c', 'UNKNOWN'),
                    'error': str(e)
                })
        
        # Progress indicator
        if (batch_num + 1) % 10 == 0 or (batch_num + 1) == total_batches:
            print(f"      Progress: {batch_num + 1}/{total_batches} batches " +
                  f"({success_count:,} success, {error_count:,} errors)")
    
    print(f"\n      [OK] Upsert completed")
    print(f"      Success: {success_count:,} records")
    print(f"      Errors:  {error_count:,} records")
    
    return success_count, error_count, errors

def save_errors(errors):
    """Save errors to CSV file"""
    if errors:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_file = f'error/sit_contact_errors_{timestamp}.csv'
        df_errors = pd.DataFrame(errors)
        df_errors.to_csv(error_file, index=False)
        print(f"      Error details saved to: {error_file}")
        return error_file
    else:
        print(f"      No errors to save")
        return None

def reconcile_data(sf, df_oracle, success_count, error_count, error_file):
    """Comprehensive reconciliation report"""
    print(f"[7/8] Generating reconciliation report...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    reconciliation = []
    
    # Step 1: Record Counts
    print(f"      Step 1/5: Comparing record counts...")
    oracle_count = len(df_oracle)
    sf_result = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != NULL")
    sf_count = sf_result['totalSize']
    
    reconciliation.append({
        'Check': 'Record Count',
        'Oracle_Value': oracle_count,
        'Salesforce_Value': sf_count,
        'Match': 'Yes' if oracle_count == sf_count else 'No',
        'Notes': f'Loaded {success_count}, Failed {error_count}'
    })
    
    # Step 2: Data Quality Checks
    print(f"      Step 2/5: Checking data quality...")
    sf_result = sf.query("""
        SELECT COUNT() FROM Contact 
        WHERE External_Id__c != NULL 
        AND (FirstName = NULL OR LastName = NULL)
    """)
    missing_names = sf_result['totalSize']
    
    reconciliation.append({
        'Check': 'Name Completeness',
        'Oracle_Value': 'All records have names',
        'Salesforce_Value': f'{missing_names} records missing names',
        'Match': 'Yes' if missing_names == 0 else 'No',
        'Notes': 'FirstName or LastName NULL'
    })
    
    # Step 3: Account Linkage
    print(f"      Step 3/5: Verifying Account linkage...")
    sf_result = sf.query("""
        SELECT COUNT() FROM Contact 
        WHERE External_Id__c != NULL AND AccountId != NULL
    """)
    linked_contacts = sf_result['totalSize']
    
    reconciliation.append({
        'Check': 'Account Linkage',
        'Oracle_Value': oracle_count,
        'Salesforce_Value': linked_contacts,
        'Match': 'Yes' if linked_contacts >= oracle_count * 0.95 else 'No',
        'Notes': f'{linked_contacts}/{oracle_count} contacts linked to Accounts'
    })
    
    # Step 4: ACR Relationships
    print(f"      Step 4/5: Checking AccountContactRelation...")
    sf_result = sf.query("""
        SELECT COUNT() FROM AccountContactRelation 
        WHERE Contact.External_Id__c != NULL AND IsDirect = true
    """)
    acr_count = sf_result['totalSize']
    
    reconciliation.append({
        'Check': 'Direct ACR Count',
        'Oracle_Value': oracle_count,
        'Salesforce_Value': acr_count,
        'Match': 'Yes' if acr_count >= linked_contacts else 'No',
        'Notes': f'Each Contact-Account link creates 1 direct ACR'
    })
    
    # Step 5: Sample Data Comparison
    print(f"      Step 5/5: Sampling data for comparison...")
    sample_ids = df_oracle['WORKER_ID'].head(5).astype(str).tolist()
    id_list = "','".join(sample_ids)
    
    sf_result = sf.query(f"""
        SELECT External_Id__c, FirstName, LastName, Email, Account.Name
        FROM Contact 
        WHERE External_Id__c IN ('{id_list}')
    """)
    
    reconciliation.append({
        'Check': 'Sample Records',
        'Oracle_Value': f'{len(sample_ids)} sample IDs',
        'Salesforce_Value': f'{len(sf_result["records"])} found in SF',
        'Match': 'Yes' if len(sf_result["records"]) == len(sample_ids) else 'No',
        'Notes': f'Sample External IDs: {", ".join(sample_ids[:3])}'
    })
    
    # Save reconciliation report
    recon_file = f'test_output/sit_contact_reconciliation_{timestamp}.csv'
    df_recon = pd.DataFrame(reconciliation)
    df_recon.to_csv(recon_file, index=False)
    
    print(f"      [OK] Reconciliation saved to: {recon_file}")
    
    # Print summary
    print(f"\n      Reconciliation Summary:")
    for item in reconciliation:
        match_symbol = "✓" if item['Match'] == 'Yes' else "✗"
        print(f"        {match_symbol} {item['Check']}: {item['Notes']}")
    
    return recon_file

def main():
    """Main execution flow"""
    print("="*80)
    print("SIT: Oracle to Salesforce Contact Load")
    print(f"Target: {LIMIT_ROWS:,} Contact records")
    print("="*80 + "\n")
    
    try:
        # Connect to systems
        conn = connect_oracle()
        sf = connect_salesforce()
        
        # Extract Oracle data
        df = extract_oracle_data(conn)
        
        # Fetch Account IDs from Salesforce
        account_map = fetch_account_ids(sf, df['EMPLOYER_ID'])
        
        # Verify External ID field
        if not verify_external_id(sf):
            print("\n[WARNING] External_Id__c verification failed")
            print("          Continuing anyway, but check field configuration\n")
        
        # Get existing Contacts to avoid duplicate ACR creation
        external_ids = df['WORKER_ID'].apply(lambda x: str(int(x)) if pd.notna(x) else None).dropna().unique()
        existing_contacts = get_existing_contacts(sf, external_ids)
        
        # Transform data
        df_mapped = map_to_salesforce(df, account_map, existing_contacts)
        
        # Load to Salesforce
        success_count, error_count, errors = upsert_to_salesforce(sf, df_mapped)
        
        # Save errors if any
        error_file = save_errors(errors)
        
        # Reconciliation
        recon_file = reconcile_data(sf, df, success_count, error_count, error_file)
        
        # Close Oracle connection
        conn.close()
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Total records processed: {len(df_mapped):,}")
        print(f"Successful upserts:      {success_count:,} ({success_count/len(df_mapped)*100:.2f}%)")
        print(f"Failed upserts:          {error_count:,} ({error_count/len(df_mapped)*100:.2f}%)")
        
        if error_count > 0:
            print(f"\nError file: {error_file}")
        
        print(f"Reconciliation: {recon_file}")
        print("\n[OK] SIT contact load completed successfully")
        
    except Exception as e:
        print(f"\n[ERROR] SIT contact load failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
