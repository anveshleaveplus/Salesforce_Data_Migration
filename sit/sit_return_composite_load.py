"""
SIT - Return__c Load with ServiceReport__c Children using Composite API
Production version: Loads Return__c with child ServiceReport__c records
Filters for active returns (PERIOD_END >= 202301)
Uses Composite API for optimal performance (25 sub-requests per composite call)
Relationships via External IDs
"""

import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
import oracledb
import pandas as pd
from simple_salesforce import Salesforce
import requests

# Load environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

# Configuration
LIMIT_RETURNS = 100  # Number of Returns to process
COMPOSITE_BATCH_SIZE = 5  # Returns per composite request (1 return + ~4 service reports = 5 operations)
ACTIVE_PERIOD = 202301

print("="*70)
print("SIT - Return__c + ServiceReport__c Composite API Load")
print("Filter: Returns with PERIOD_END >= 202301 (Jan 2023)")
print("="*70)
print(f"Loading: {LIMIT_RETURNS} Returns with child Service Reports")
print(f"Composite batch size: {COMPOSITE_BATCH_SIZE} Returns per request")
print(f"Active period filter: >= {ACTIVE_PERIOD}")
print()

def connect_oracle():
    """Connect to Oracle database"""
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
        print(f"      [ERROR] {e}")
        sys.exit(1)

def connect_salesforce():
    """Connect to Salesforce"""
    print("[2/6] Connecting to Salesforce...")
    try:
        sf = Salesforce(
            username=os.getenv('SF_USERNAME'),
            password=os.getenv('SF_PASSWORD'),
            security_token=os.getenv('SF_SECURITY_TOKEN'),
            domain=os.getenv('SF_DOMAIN', 'test')
        )
        print(f"      [OK] Connected to {sf.sf_instance}")
        return sf
    except Exception as e:
        print(f"      [ERROR] {e}")
        sys.exit(1)

def extract_returns_with_service_data(conn, limit=100):
    """
    Extract Return (WSR) data with associated Service Report data
    Returns: DataFrame with Return data and dict of Service Reports by WSR_ID
    """
    print(f"[3/6] Extracting Return and Service Report data...")
    
    # Get Return (WSR) data
    return_query = f"""
    SELECT 
        wsr.WSR_ID,
        wsr.EMPLOYER_ID,
        wsr.DATE_RECEIVED,
        wsr.EVENT_TYPE_CODE,
        wsr.EMPLOYER_DAYS as TOTAL_DAYS,
        wsr.EMPLOYER_TOTAL_WAGES,
        wsr.PERIOD_START,
        wsr.PERIOD_END,
        wsrt.CONTRIBUTION_AMOUNT,
        inv.PAYMENT_DUE_DATE,
        inv_det.AMOUNT as INVOICE_AMOUNT
    FROM SCH_CO_20.CO_WSR wsr
    LEFT JOIN SCH_CO_20.CO_WSR_TOTALS wsrt ON wsrt.WSR_ID = wsr.WSR_ID
    LEFT JOIN SCH_CO_20.CO_ACC_INVOICE_DETAIL inv_det ON inv_det.WSR_ID = wsr.WSR_ID
    LEFT JOIN SCH_CO_20.CO_ACC_INVOICE inv ON inv.INVOICE_ID = inv_det.INVOICE_ID
    WHERE wsr.EMPLOYER_ID != 23000
    AND wsr.PERIOD_END >= {ACTIVE_PERIOD}
    AND ROWNUM <= {limit}
    ORDER BY wsr.WSR_ID
    """
    
    df_returns = pd.read_sql(return_query, conn)
    print(f"      Extracted {len(df_returns):,} Return records")
    
    # Get Service Report data for these Returns
    wsr_ids = df_returns['WSR_ID'].tolist()
    wsr_ids_str = ','.join([str(int(x)) for x in wsr_ids])
    
    service_query = f"""
    SELECT 
        s.WSR_ID,
        s.WORKER as WORKER_ID,
        s.PERIOD_END as SERVICE_PERIOD,
        s.DAYS_WORKED,
        s.WAGES,
        s.SERVICE_ID
    FROM SCH_CO_20.CO_SERVICE s
    WHERE s.WSR_ID IN ({wsr_ids_str})
    AND s.PERIOD_END >= {ACTIVE_PERIOD}
    ORDER BY s.WSR_ID, s.WORKER
    """
    
    df_services = pd.read_sql(service_query, conn)
    print(f"      Extracted {len(df_services):,} Service Report records")
    
    # Group services by WSR_ID
    services_by_return = {}
    for wsr_id, group in df_services.groupby('WSR_ID'):
        services_by_return[int(wsr_id)] = group.to_dict('records')
    
    print(f"      Grouped services for {len(services_by_return)} Returns")
    print(f"      Avg services per return: {len(df_services)/len(df_returns):.1f}")
    
    return df_returns, services_by_return

def build_composite_request(returns_batch, services_by_return):
    """
    Build Composite API request for a batch of Returns and their Service Reports
    Max 25 sub-requests per Composite request
    """
    
    composite_request = {
        "allOrNone": False,
        "compositeRequest": []
    }
    
    operation_count = 0
    max_operations = 25  # Salesforce limit
    
    for idx, return_row in returns_batch.iterrows():
        wsr_id = int(return_row['WSR_ID'])
        employer_id = str(int(return_row['EMPLOYER_ID'])) if pd.notna(return_row['EMPLOYER_ID']) else None
        
        if not employer_id:
            continue
        
        # Check if we have room for this Return + its services
        services = services_by_return.get(wsr_id, [])
        total_ops = 1 + len(services)  # 1 Return + N services
        
        if operation_count + total_ops > max_operations:
            break  # Batch is full
        
        reference_id = f"return_{wsr_id}"
        
        # 1. UPSERT Return__c
        return_body = {
            "External_Id__c": str(wsr_id),
            "Employer__r": {"External_Id__c": employer_id}
        }
        
        # Add optional fields
        if pd.notna(return_row['DATE_RECEIVED']):
            return_body["ReturnSubmittedDate__c"] = return_row['DATE_RECEIVED'].strftime('%Y-%m-%d')
        if pd.notna(return_row['TOTAL_DAYS']):
            return_body["TotalDaysWorked__c"] = int(return_row['TOTAL_DAYS'])
            return_body["TotalDaysReported__c"] = int(return_row['TOTAL_DAYS'])
        if pd.notna(return_row['EMPLOYER_TOTAL_WAGES']):
            return_body["TotalWagesReported__c"] = float(return_row['EMPLOYER_TOTAL_WAGES'])
        if pd.notna(return_row['CONTRIBUTION_AMOUNT']):
            return_body["Charges__c"] = float(return_row['CONTRIBUTION_AMOUNT'])
        if pd.notna(return_row['INVOICE_AMOUNT']):
            return_body["InvoiceAmount__c"] = float(return_row['INVOICE_AMOUNT'])
            return_body["AmountPayable__c"] = float(return_row['INVOICE_AMOUNT'])
        if pd.notna(return_row['PAYMENT_DUE_DATE']):
            return_body["InvoiceDueDate__c"] = return_row['PAYMENT_DUE_DATE'].strftime('%Y-%m-%d')
        
        composite_request["compositeRequest"].append({
            "method": "PATCH",
            "url": f"/services/data/v59.0/sobjects/Return__c/External_Id__c/{wsr_id}",
            "referenceId": reference_id,
            "body": return_body
        })
        operation_count += 1
        
        # 2. CREATE ServiceReport__c children
        for service_idx, service in enumerate(services):
            worker_id = str(int(service['WORKER_ID'])) if pd.notna(service['WORKER_ID']) else None
            service_id = int(service['SERVICE_ID'])
            
            if not worker_id:
                continue
            
            service_body = {
                "External_Id__c": f"{wsr_id}_{service_id}",
                "Return__c": f"@{{{reference_id}.id}}",  # Reference parent Return
                "Worker__r": {"External_Id__c": worker_id}
            }
            
            # Add optional fields
            if pd.notna(service['SERVICE_PERIOD']):
                service_body["ServicePeriod__c"] = str(int(service['SERVICE_PERIOD']))
            if pd.notna(service['DAYS_WORKED']):
                service_body["DaysWorked__c"] = int(service['DAYS_WORKED'])
            if pd.notna(service['WAGES']):
                service_body["WagesEarned__c"] = float(service['WAGES'])
            
            composite_request["compositeRequest"].append({
                "method": "POST",
                "url": "/services/data/v59.0/sobjects/ServiceReport__c",
                "referenceId": f"service_{wsr_id}_{service_idx}",
                "body": service_body
            })
            operation_count += 1
    
    return composite_request

def execute_composite_request(sf, composite_request):
    """Execute Composite API request"""
    
    composite_url = f"https://{sf.sf_instance}/services/data/v59.0/composite"
    
    headers = {
        "Authorization": f"Bearer {sf.session_id}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            composite_url,
            headers=headers,
            json=composite_request,
            timeout=120
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Count successes and failures
            success_count = 0
            error_count = 0
            errors = []
            
            for sub_response in result.get('compositeResponse', []):
                http_status = sub_response.get('httpStatusCode')
                if http_status in [200, 201, 204]:
                    success_count += 1
                else:
                    error_count += 1
                    errors.append({
                        'referenceId': sub_response.get('referenceId'),
                        'httpStatus': http_status,
                        'error': sub_response.get('body')
                    })
            
            return {
                'success': True,
                'success_count': success_count,
                'error_count': error_count,
                'errors': errors,
                'response': result
            }
        else:
            return {
                'success': False,
                'error': f"HTTP {response.status_code}: {response.text}"
            }
            
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

def process_returns_composite(sf, df_returns, services_by_return, batch_size=5):
    """Process Returns with Composite API in batches"""
    print(f"[4/6] Processing with Composite API...")
    print(f"      Batch size: {batch_size} Returns per composite request\n")
    
    total_returns = len(df_returns)
    total_success = 0
    total_errors = 0
    all_errors = []
    
    start_time = datetime.now()
    
    for i in range(0, total_returns, batch_size):
        batch_num = (i // batch_size) + 1
        batch = df_returns.iloc[i:i+batch_size]
        
        print(f"  Batch {batch_num} ({len(batch)} Returns)...", end=" ")
        
        # Build composite request
        composite_request = build_composite_request(batch, services_by_return)
        operations_count = len(composite_request['compositeRequest'])
        
        print(f"{operations_count} operations...", end=" ")
        
        # Execute
        result = execute_composite_request(sf, composite_request)
        
        if result['success']:
            total_success += result['success_count']
            total_errors += result['error_count']
            all_errors.extend(result['errors'])
            
            print(f"[OK] {result['success_count']} success, {result['error_count']} errors")
        else:
            total_errors += operations_count
            print(f"[ERROR] {result['error']}")
            all_errors.append({
                'batch': batch_num,
                'error': result['error']
            })
    
    elapsed = datetime.now() - start_time
    
    print(f"\n{'='*70}")
    print("COMPOSITE API LOAD SUMMARY")
    print(f"{'='*70}")
    print(f"Returns processed: {total_returns:,}")
    print(f"Total operations: {total_success + total_errors:,}")
    print(f"Successful: {total_success:,}")
    print(f"Errors: {total_errors:,}")
    print(f"Time taken: {elapsed}")
    print(f"Rate: {(total_success + total_errors)/elapsed.total_seconds():.1f} operations/sec")
    
    if all_errors and len(all_errors) <= 20:
        print(f"\nError details:")
        for err in all_errors:
            print(f"  {err.get('referenceId', 'Unknown')}: {err.get('error', err)}")
    
    return {
        'success_count': total_success,
        'error_count': total_errors,
        'errors': all_errors
    }

def main():
    """Main execution"""
    
    # Connect to databases
    conn = connect_oracle()
    sf = connect_salesforce()
    
    # Extract data
    df_returns, services_by_return = extract_returns_with_service_data(conn, LIMIT_RETURNS)
    conn.close()
    
    if len(df_returns) == 0:
        print("[WARNING] No data to process")
        sys.exit(0)
    
    # Process with Composite API
    result = process_returns_composite(sf, df_returns, services_by_return, COMPOSITE_BATCH_SIZE)
    
    # Save errors if any
    if result['errors']:
        error_file = f"sit_return_composite_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(error_file, 'w') as f:
            json.dump(result['errors'], f, indent=2)
        print(f"\nError details saved to: {error_file}")
    
    print("\n[COMPLETE] Composite API load finished")

if __name__ == '__main__':
    main()
