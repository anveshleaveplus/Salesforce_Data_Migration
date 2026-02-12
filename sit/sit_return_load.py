"""
SIT - Salesforce Return__c Object Load
Loads Return (Weekly Service Return) records from Oracle to Salesforce SIT environment
Links to employer accounts via CO_WSR.EMPLOYER_ID
Filters for active returns (PERIOD_END >= 202301)
Includes invoice, charges, and interest data
"""

import os
import sys
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
LIMIT_ROWS = 50000  # Load first 50K returns
BATCH_SIZE = 500
ACTIVE_PERIOD = 202301  # Filter for returns >= Jan 2023

print("="*70)
print("SIT - Oracle to Salesforce Return__c Load")
print("Loading: Weekly Service Return (WSR) records")
print("Filter: Returns with PERIOD_END >= 202301 (Jan 2023)")
print("="*70)
print(f"Loading: {LIMIT_ROWS:,} returns" if LIMIT_ROWS else "Loading: All active returns")
print(f"Batch size: {BATCH_SIZE}")
print(f"Active period filter: >= {ACTIVE_PERIOD}")
print()

def connect_oracle():
    """Establish Oracle database connection"""
    print("[1/6] Connecting to Oracle...")
    try:
        conn = oracledb.connect(
            user=os.getenv('ORACLE_USER'),
            password=os.getenv('ORACLE_PASSWORD'),
            host=os.getenv('ORACLE_HOST'),
            port=int(os.getenv('ORACLE_PORT')),
            sid=os.getenv('ORACLE_SID')
        )
        print("      [OK] Oracle connected")
        return conn
    except Exception as e:
        print(f"      [ERROR] Oracle connection failed: {e}")
        sys.exit(1)

def connect_salesforce():
    """Establish Salesforce connection"""
    print("[2/6] Connecting to Salesforce...")
    try:
        sf = Salesforce(
            username=os.getenv('SF_USERNAME'),
            password=os.getenv('SF_PASSWORD'),
            security_token=os.getenv('SF_SECURITY_TOKEN'),
            domain=os.getenv('SF_DOMAIN', 'test')
        )
        print("      [OK] Salesforce connected")
        print(f"      Instance: {sf.sf_instance}")
        return sf
    except Exception as e:
        print(f"      [ERROR] Salesforce connection failed: {e}")
        sys.exit(1)

def fetch_account_ids(sf, employer_ids):
    """Fetch Salesforce Account IDs for given External_Id__c values"""
    print("[3/6] Fetching Account IDs from Salesforce...")
    
    # Remove None values and convert to set
    valid_employer_ids = set(str(int(x)) for x in employer_ids if pd.notna(x))
    
    if not valid_employer_ids:
        print("      [WARNING] No valid employer IDs to fetch")
        return {}
    
    # Query Accounts in batches (SOQL IN clause limit is 4000)
    account_map = {}  # employer_external_id -> salesforce_account_id
    batch_size = 500  # Reduced from 2000 to avoid URI too long error
    employer_list = list(valid_employer_ids)
    
    print(f"      Fetching {len(employer_list):,} Account IDs in batches of {batch_size}...")
    
    for i in range(0, len(employer_list), batch_size):
        batch = employer_list[i:i+batch_size]
        ids_str = "','".join(batch)
        
        query = f"""
            SELECT Id, External_Id__c 
            FROM Account 
            WHERE External_Id__c IN ('{ids_str}')
        """
        
        result = sf.query(query)
        for record in result['records']:
            account_map[record['External_Id__c']] = record['Id']
    
    print(f"      [OK] Found {len(account_map):,} matching Accounts")
    
    missing = len(valid_employer_ids) - len(account_map)
    if missing > 0:
        print(f"      [WARNING] {missing:,} employer IDs not found in Salesforce Accounts")
    
    return account_map

def load_picklist_mappings(cursor):
    """Load code mappings for picklist fields from CO_CODE table"""
    print("[4/6] Loading picklist value mappings...")
    
    picklist_mappings = {}
    
    # Return Type Code (EVENT_TYPE_CODE)
    try:
        cursor.execute("""
            SELECT code_set_id 
            FROM SCH_CO_20.CO_CODE_SET 
            WHERE LOWER(code_set_name) LIKE 'eventtypecode%'
        """)
        result = cursor.fetchone()
        
        if result:
            code_set_id = result[0]
            cursor.execute(f"""
                SELECT value, description 
                FROM SCH_CO_20.CO_CODE 
                WHERE code_set_id = {code_set_id}
            """)
            picklist_mappings['EVENT_TYPE_CODE'] = {row[0]: row[1] for row in cursor.fetchall()}
            print(f"      Loaded {len(picklist_mappings['EVENT_TYPE_CODE'])} Return Type values")
        else:
            print("      [WARNING] EVENT_TYPE_CODE mapping not found")
            picklist_mappings['EVENT_TYPE_CODE'] = {}
    except Exception as e:
        print(f"      [ERROR] Loading EVENT_TYPE_CODE: {e}")
        picklist_mappings['EVENT_TYPE_CODE'] = {}
    
    # Invoice Status Code
    try:
        cursor.execute("""
            SELECT code_set_id 
            FROM SCH_CO_20.CO_CODE_SET 
            WHERE LOWER(code_set_name) LIKE 'invoicestatuscode%' OR LOWER(code_set_name) LIKE '%status%'
        """)
        result = cursor.fetchone()
        
        if result:
            code_set_id = result[0]
            cursor.execute(f"""
                SELECT value, description 
                FROM SCH_CO_20.CO_CODE 
                WHERE code_set_id = {code_set_id}
            """)
            picklist_mappings['STATUS_CODE'] = {row[0]: row[1] for row in cursor.fetchall()}
            print(f"      Loaded {len(picklist_mappings['STATUS_CODE'])} Invoice Status values")
        else:
            print("      [WARNING] Invoice STATUS_CODE mapping not found")
            picklist_mappings['STATUS_CODE'] = {}
    except Exception as e:
        print(f"      [ERROR] Loading STATUS_CODE: {e}")
        picklist_mappings['STATUS_CODE'] = {}
    
    print(f"      [OK] Loaded {len(picklist_mappings)} picklist mapping tables")
    return picklist_mappings

def extract_return_data(conn, limit_rows=None):
    """Extract Return data from Oracle CO_WSR and related tables"""
    print(f"[5/6] Extracting Return data from Oracle...")
    
    # Build LIMIT clause
    limit_clause = f"AND ROWNUM <= {limit_rows}" if limit_rows else ""
    
    query = f"""
    SELECT 
        wsr.WSR_ID,
        wsr.EMPLOYER_ID,
        wsr.DATE_RECEIVED as RETURN_SUBMITTED_DATE,
        wsr.EVENT_TYPE_CODE,
        wsr.EMPLOYER_DAYS as TOTAL_DAYS_WORKED,
        wsr.EMPLOYER_TOTAL_WAGES as TOTAL_WAGES_REPORTED,
        wsr.PERIOD_START,
        wsr.PERIOD_END,
        
        -- WSR Totals (Charges)
        wsrt.CONTRIBUTION_AMOUNT as CHARGES,
        
        -- Adjustment (Interest)
        adj.STATUATORY_INTEREST as INTEREST,
        
        -- Invoice details
        inv.PAYMENT_DUE_DATE as INVOICE_DUE_DATE,
        inv_det.AMOUNT as INVOICE_AMOUNT,
        inv_det.STATUS_CODE as INVOICE_STATUS_CODE
        
    FROM SCH_CO_20.CO_WSR wsr
    
    LEFT JOIN SCH_CO_20.CO_WSR_TOTALS wsrt
        ON wsrt.WSR_ID = wsr.WSR_ID
    
    LEFT JOIN (
        SELECT 
            EMPLOYER_ID,
            SUM(STATUATORY_INTEREST) as STATUATORY_INTEREST
        FROM SCH_CO_20.CO_ADJUSTMENT
        GROUP BY EMPLOYER_ID
    ) adj ON adj.EMPLOYER_ID = wsr.EMPLOYER_ID
    
    LEFT JOIN SCH_CO_20.CO_ACC_INVOICE inv
        ON inv.CUSTOMER_ID = wsr.EMPLOYER_ID
        AND inv.WSR_PERIOD = wsr.PERIOD_END
    
    LEFT JOIN SCH_CO_20.CO_ACC_INVOICE_DETAIL inv_det
        ON inv_det.INVOICE_ID = inv.INVOICE_ID
    
    WHERE wsr.EMPLOYER_ID != 23000  -- Exclude LSL Credits internal account
    AND wsr.PERIOD_END >= {ACTIVE_PERIOD}  -- Filter for active period (>= Jan 2023)
    {limit_clause}
    
    ORDER BY wsr.WSR_ID
    """
    
    try:
        df = pd.read_sql(query, conn)
        print(f"      [OK] Extracted {len(df):,} return records")
        
        # Show sample data
        if len(df) > 0:
            print(f"\n      Sample data (first 3 records):")
            print(f"      WSR_IDs: {df['WSR_ID'].head(3).tolist()}")
            print(f"      Employer IDs: {df['EMPLOYER_ID'].head(3).tolist()}")
            print(f"      Return dates: {df['RETURN_SUBMITTED_DATE'].head(3).tolist()}")
            
            # Data quality checks
            null_employer = df['EMPLOYER_ID'].isna().sum()
            null_wsr = df['WSR_ID'].isna().sum()
            
            print(f"\n      Data quality:")
            print(f"        NULL EMPLOYER_ID: {null_employer:,} ({null_employer/len(df)*100:.1f}%)")
            print(f"        NULL WSR_ID: {null_wsr:,} ({null_wsr/len(df)*100:.1f}%)")
            
            # Check for duplicate WSR_IDs (External_Id__c must be unique)
            duplicates = df[df.duplicated(subset=['WSR_ID'], keep=False)]
            if len(duplicates) > 0:
                print(f"\n      [WARNING] Found {len(duplicates):,} duplicate WSR_IDs!")
                print(f"        Unique WSR_IDs with duplicates: {duplicates['WSR_ID'].nunique()}")
                print(f"        Keeping first occurrence, dropping {len(duplicates) - duplicates['WSR_ID'].nunique()} duplicates")
                
                # Show sample duplicates
                dup_sample = duplicates.groupby('WSR_ID').size().sort_values(ascending=False).head(5)
                print(f"\n        Top duplicate WSR_IDs:")
                for wsr_id, count in dup_sample.items():
                    print(f"          WSR_ID {wsr_id}: {count} occurrences")
                
                # Remove duplicates - keep first occurrence
                df = df.drop_duplicates(subset=['WSR_ID'], keep='first')
                print(f"\n      [OK] After deduplication: {len(df):,} records")
            
            # Check for currency field overflows (Salesforce max: 999,999,999,999.99)
            currency_max = 999999999999.99
            currency_fields = ['TOTAL_WAGES_REPORTED', 'CHARGES', 'INTEREST', 'INVOICE_AMOUNT']
            
            overflow_issues = []
            for field in currency_fields:
                if field in df.columns:
                    overflow = df[df[field] > currency_max]
                    if len(overflow) > 0:
                        max_val = df[field].max()
                        overflow_issues.append(f"{field}: {len(overflow)} records exceed limit (max: {max_val:,.2f})")
            
            if overflow_issues:
                print(f"\n      [WARNING] Currency field overflow detected:")
                for issue in overflow_issues:
                    print(f"        {issue}")
                print(f"        These records will fail in Salesforce (Currency max: {currency_max:,.2f})")
        
        return df
        
    except Exception as e:
        print(f"      [ERROR] Failed to extract data: {e}")
        sys.exit(1)

def map_to_salesforce(df, account_map, picklist_mappings):
    """Map Oracle columns to Salesforce Return__c fields"""
    print("[6/6] Mapping to Salesforce Return__c fields...")
    
    mapped_records = []
    skipped_no_account = 0
    skipped_invalid_data = 0
    
    for idx, row in df.iterrows():
        employer_id = str(int(row['EMPLOYER_ID'])) if pd.notna(row['EMPLOYER_ID']) else None
        
        # Skip if no employer ID or account not found
        if not employer_id or employer_id not in account_map:
            skipped_no_account += 1
            continue
        
        try:
            # Build Salesforce record
            sf_record = {
                'External_Id__c': str(int(row['WSR_ID'])) if pd.notna(row['WSR_ID']) else None,
                'Employer__c': account_map[employer_id],
                'ReturnSubmittedDate__c': row['RETURN_SUBMITTED_DATE'].strftime('%Y-%m-%d') if pd.notna(row['RETURN_SUBMITTED_DATE']) else None,
                'TotalDaysWorked__c': int(row['TOTAL_DAYS_WORKED']) if pd.notna(row['TOTAL_DAYS_WORKED']) else None,
                'TotalDaysReported__c': int(row['TOTAL_DAYS_WORKED']) if pd.notna(row['TOTAL_DAYS_WORKED']) else None,  # Same field
                'TotalWagesReported__c': float(row['TOTAL_WAGES_REPORTED']) if pd.notna(row['TOTAL_WAGES_REPORTED']) else None,
                'Charges__c': float(row['CHARGES']) if pd.notna(row['CHARGES']) else None,
                'Interest__c': float(row['INTEREST']) if pd.notna(row['INTEREST']) else None,
                'InvoiceAmount__c': float(row['INVOICE_AMOUNT']) if pd.notna(row['INVOICE_AMOUNT']) else None,
                'AmountPayable__c': float(row['INVOICE_AMOUNT']) if pd.notna(row['INVOICE_AMOUNT']) else None,  # Same as invoice amount
                'InvoiceDueDate__c': row['INVOICE_DUE_DATE'].strftime('%Y-%m-%d') if pd.notna(row['INVOICE_DUE_DATE']) else None,
            }
            
            # Map picklist values
            if pd.notna(row['EVENT_TYPE_CODE']):
                event_code = row['EVENT_TYPE_CODE']
                mapped_value = picklist_mappings['EVENT_TYPE_CODE'].get(event_code)
                if mapped_value:
                    sf_record['ReturnType__c'] = mapped_value
            
            # Skip InvoiceStatus__c - Oracle values don't match Salesforce picklist
            # Salesforce only accepts: "Not Created", "Generating", "Sent"
            # Oracle has: "Due" (and others) which are invalid
            
            # Remove None values for cleaner API calls
            sf_record = {k: v for k, v in sf_record.items() if v is not None}
            
            mapped_records.append(sf_record)
            
        except Exception as e:
            # Skip records with data conversion errors
            skipped_invalid_data += 1
            if skipped_invalid_data <= 5:  # Show first 5 errors
                print(f"      [WARNING] Skipped WSR_ID {row.get('WSR_ID')}: {e}")
    
    print(f"      [OK] Mapped {len(mapped_records):,} records")
    print(f"      Skipped {skipped_no_account:,} records (no Account found)")
    if skipped_invalid_data > 0:
        print(f"      Skipped {skipped_invalid_data:,} records (data conversion errors)")
    
    # Show sample mapped data
    if mapped_records:
        print(f"\n      Sample mapped record:")
        for key, value in list(mapped_records[0].items())[:8]:
            print(f"        {key}: {value}")
    
    return mapped_records
    
    # Show sample mapped data
    if mapped_records:
        print(f"\n      Sample mapped record:")
        for key, value in list(mapped_records[0].items())[:8]:
            print(f"        {key}: {value}")
    
    return mapped_records

def upsert_to_salesforce(sf, records, batch_size=500):
    """Upsert records to Salesforce Return__c object using External_Id__c"""
    print(f"\nUpserting {len(records):,} records to Salesforce Return__c...")
    print(f"  External ID field: External_Id__c")
    print(f"  Batch size: {batch_size}")
    
    # Verify External_Id__c field exists and is configured
    try:
        return_metadata = sf.Return__c.describe()
        external_id_field = next((f for f in return_metadata['fields'] if f['name'] == 'External_Id__c'), None)
        
        if not external_id_field:
            print("\n[ERROR] External_Id__c field does not exist on Return__c object")
            print("\nPlease create the field first in Salesforce Setup")
            sys.exit(1)
        
        if not external_id_field.get('externalId'):
            print("\n[ERROR] External_Id__c exists but is NOT marked as External ID")
            print("\nPlease enable 'External ID' checkbox on the field")
            sys.exit(1)
        
        print(f"  [OK] External_Id__c is properly configured")
        
    except Exception as e:
        print(f"\n[WARNING] Could not verify External ID field: {str(e)}")
        print("  Proceeding anyway...")
    
    # Upsert in batches
    start_time = datetime.now()
    success_count = 0
    error_count = 0
    errors = []
    
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    for i in range(0, len(records), batch_size):
        batch_num = (i // batch_size) + 1
        batch = records[i:i+batch_size]
        
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} records)...", end=" ")
        
        try:
            # UPSERT using External_Id__c
            result = sf.bulk.Return__c.upsert(batch, 'External_Id__c', batch_size=batch_size, use_serial=True)
            
            # Count successes and errors
            batch_success = sum(1 for r in result if r.get('success'))
            batch_errors = len(result) - batch_success
            
            success_count += batch_success
            error_count += batch_errors
            
            # Collect error details and show first few immediately
            for idx, r in enumerate(result):
                if not r.get('success'):
                    error_record = {
                        'batch': batch_num,
                        'index': i + idx,
                        'external_id': batch[idx].get('External_Id__c'),
                        'error': r.get('errors', 'Unknown error')
                    }
                    errors.append(error_record)
                    
                    # Show first 5 errors immediately
                    if len(errors) <= 5:
                        print(f"\n      [ERROR DETAIL] External_Id: {error_record['external_id']}")
                        print(f"                     Error: {error_record['error']}")
            
            print(f"[OK] {batch_success} success, {batch_errors} errors")
            
        except Exception as e:
            error_count += len(batch)
            print(f"[ERROR] Failed: {str(e)}")
            for idx in range(len(batch)):
                error_record = {
                    'batch': batch_num,
                    'index': i + idx,
                    'external_id': batch[idx].get('External_Id__c'),
                    'error': str(e)
                }
                errors.append(error_record)
    
    elapsed_time = datetime.now() - start_time
    
    print(f"\n{'='*70}")
    print("UPSERT SUMMARY")
    print(f"{'='*70}")
    print(f"Total records processed: {len(records):,}")
    print(f"Successful: {success_count:,}")
    print(f"Errors: {error_count:,}")
    print(f"Time taken: {elapsed_time}")
    print(f"Rate: {len(records)/elapsed_time.total_seconds():.1f} records/sec")
    
    # Show error details if any
    if errors and error_count <= 20:
        print(f"\nError details:")
        for err in errors[:20]:
            print(f"  External_Id: {err['external_id']} - {err['error']}")
    elif errors:
        print(f"\nShowing first 20 errors (total: {error_count}):")
        for err in errors[:20]:
            print(f"  External_Id: {err['external_id']} - {err['error']}")
    
    return success_count, error_count, errors

def main():
    """Main execution flow"""
    
    # Connect to Oracle
    conn = connect_oracle()
    
    # Connect to Salesforce
    sf = connect_salesforce()
    
    # Extract Return data
    df = extract_return_data(conn, limit_rows=LIMIT_ROWS)
    
    if len(df) == 0:
        print("\n[WARNING] No data extracted. Exiting.")
        conn.close()
        sys.exit(0)
    
    # Load picklist mappings
    cursor = conn.cursor()
    picklist_mappings = load_picklist_mappings(cursor)
    cursor.close()
    
    # Fetch Account IDs
    employer_ids = df['EMPLOYER_ID'].dropna().unique()
    account_map = fetch_account_ids(sf, employer_ids)
    
    if not account_map:
        print("\n[ERROR] No matching Accounts found in Salesforce")
        print("Please ensure Accounts are loaded first")
        conn.close()
        sys.exit(1)
    
    # Map to Salesforce format
    sf_records = map_to_salesforce(df, account_map, picklist_mappings)
    
    if not sf_records:
        print("\n[WARNING] No records to upsert. Exiting.")
        conn.close()
        sys.exit(0)
    
    # Close Oracle connection
    conn.close()
    
    # Upsert to Salesforce
    success, errors, error_details = upsert_to_salesforce(sf, sf_records, BATCH_SIZE)
    
    # Save error details if any
    if error_details:
        error_file = f'sit_return_load_errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        pd.DataFrame(error_details).to_csv(error_file, index=False)
        print(f"\nError details saved to: {error_file}")
    
    print("\n[COMPLETE] Return__c load finished")

if __name__ == '__main__':
    main()
