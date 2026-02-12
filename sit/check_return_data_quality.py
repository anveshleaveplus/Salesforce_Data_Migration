"""
Check Return__c Data Quality
- Oracle data types for source columns
- Duplicate WSR_IDs
- NULL values
- Data type compatibility with Salesforce
"""

import os
from dotenv import load_dotenv
import oracledb
import pandas as pd

# Load environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

ACTIVE_PERIOD = 202301

print("="*70)
print("Return__c Data Quality Check")
print("="*70)

# Connect to Oracle
print("\n[1/4] Connecting to Oracle...")
try:
    conn = oracledb.connect(
        user=os.getenv('ORACLE_USER'),
        password=os.getenv('ORACLE_PASSWORD'),
        host=os.getenv('ORACLE_HOST'),
        port=int(os.getenv('ORACLE_PORT')),
        sid=os.getenv('ORACLE_SID')
    )
    print("      [OK] Connected")
except Exception as e:
    print(f"      [ERROR] {e}")
    exit(1)

# Check Oracle data types
print("\n[2/4] Checking Oracle column data types...")
cursor = conn.cursor()

tables_columns = {
    'CO_WSR': ['WSR_ID', 'EMPLOYER_ID', 'DATE_RECEIVED', 'EVENT_TYPE_CODE', 
               'EMPLOYER_DAYS', 'EMPLOYER_TOTAL_WAGES', 'PERIOD_START', 'PERIOD_END'],
    'CO_WSR_TOTALS': ['CONTRIBUTION_AMOUNT'],
    'CO_ADJUSTMENT': ['STATUATORY_INTEREST'],
    'CO_ACC_INVOICE_DETAIL': ['AMOUNT', 'STATUS_CODE'],
    'CO_ACC_INVOICE': ['PAYMENT_DUE_DATE']
}

print("\n      Oracle Column Data Types:")
print(f"      {'Table':<25} {'Column':<30} {'Data Type':<20}")
print(f"      {'-'*25} {'-'*30} {'-'*20}")

for table, columns in tables_columns.items():
    for column in columns:
        try:
            cursor.execute(f"""
                SELECT DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = 'SCH_CO_20'
                AND TABLE_NAME = '{table}'
                AND COLUMN_NAME = '{column}'
            """)
            result = cursor.fetchone()
            
            if result:
                data_type = result[0]
                length = result[1]
                precision = result[2]
                scale = result[3]
                
                if data_type == 'NUMBER' and precision:
                    type_str = f"NUMBER({precision},{scale if scale else 0})"
                elif data_type == 'VARCHAR2':
                    type_str = f"VARCHAR2({length})"
                else:
                    type_str = data_type
                
                print(f"      {table:<25} {column:<30} {type_str:<20}")
            else:
                print(f"      {table:<25} {column:<30} {'NOT FOUND':<20}")
        except Exception as e:
            print(f"      {table:<25} {column:<30} ERROR: {e}")

cursor.close()

# Check WSR_ID duplicates
print("\n[3/4] Checking for duplicate WSR_IDs...")
query_duplicates = f"""
SELECT 
    WSR_ID,
    COUNT(*) as duplicate_count
FROM SCH_CO_20.CO_WSR
WHERE EMPLOYER_ID != 23000
AND PERIOD_END >= {ACTIVE_PERIOD}
GROUP BY WSR_ID
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
"""

try:
    df_duplicates = pd.read_sql(query_duplicates, conn)
    
    if len(df_duplicates) > 0:
        print(f"      [WARNING] Found {len(df_duplicates):,} duplicate WSR_IDs!")
        print(f"\n      Top 10 duplicates:")
        print(df_duplicates.head(10).to_string(index=False))
    else:
        print(f"      [OK] No duplicate WSR_IDs found")
except Exception as e:
    print(f"      [ERROR] {e}")

# Check data quality issues
print("\n[4/4] Checking data quality for Return fields...")
quality_query = f"""
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT wsr.WSR_ID) as unique_wsr_ids,
    SUM(CASE WHEN wsr.WSR_ID IS NULL THEN 1 ELSE 0 END) as null_wsr_id,
    SUM(CASE WHEN wsr.EMPLOYER_ID IS NULL THEN 1 ELSE 0 END) as null_employer_id,
    SUM(CASE WHEN wsr.DATE_RECEIVED IS NULL THEN 1 ELSE 0 END) as null_date_received,
    SUM(CASE WHEN wsr.EMPLOYER_DAYS IS NULL THEN 1 ELSE 0 END) as null_days,
    SUM(CASE WHEN wsr.EMPLOYER_TOTAL_WAGES IS NULL THEN 1 ELSE 0 END) as null_wages,
    SUM(CASE WHEN wsrt.CONTRIBUTION_AMOUNT IS NULL THEN 1 ELSE 0 END) as null_charges,
    SUM(CASE WHEN adj.STATUATORY_INTEREST IS NULL THEN 1 ELSE 0 END) as null_interest,
    SUM(CASE WHEN inv_det.AMOUNT IS NULL THEN 1 ELSE 0 END) as null_invoice_amount,
    SUM(CASE WHEN inv.PAYMENT_DUE_DATE IS NULL THEN 1 ELSE 0 END) as null_due_date,
    MIN(wsr.EMPLOYER_DAYS) as min_days,
    MAX(wsr.EMPLOYER_DAYS) as max_days,
    MIN(wsr.EMPLOYER_TOTAL_WAGES) as min_wages,
    MAX(wsr.EMPLOYER_TOTAL_WAGES) as max_wages,
    MIN(wsrt.CONTRIBUTION_AMOUNT) as min_charges,
    MAX(wsrt.CONTRIBUTION_AMOUNT) as max_charges
FROM SCH_CO_20.CO_WSR wsr
LEFT JOIN SCH_CO_20.CO_WSR_TOTALS wsrt ON wsrt.WSR_ID = wsr.WSR_ID
LEFT JOIN (
    SELECT WSR_ID, SUM(STATUATORY_INTEREST) as STATUATORY_INTEREST
    FROM SCH_CO_20.CO_ADJUSTMENT
    GROUP BY WSR_ID
) adj ON adj.WSR_ID = wsr.WSR_ID
LEFT JOIN SCH_CO_20.CO_ACC_INVOICE_DETAIL inv_det ON inv_det.WSR_ID = wsr.WSR_ID
LEFT JOIN SCH_CO_20.CO_ACC_INVOICE inv ON inv.INVOICE_ID = inv_det.INVOICE_ID
WHERE wsr.EMPLOYER_ID != 23000
AND wsr.PERIOD_END >= {ACTIVE_PERIOD}
"""

try:
    df_quality = pd.read_sql(quality_query, conn)
    
    print(f"\n      Data Quality Summary:")
    print(f"      {'Metric':<30} {'Value':<20} {'Percentage':<15}")
    print(f"      {'-'*30} {'-'*20} {'-'*15}")
    
    total = df_quality['TOTAL_RECORDS'].iloc[0]
    unique_wsr = df_quality['UNIQUE_WSR_IDS'].iloc[0]
    
    print(f"      {'Total Records':<30} {total:>19,}")
    print(f"      {'Unique WSR_IDs':<30} {unique_wsr:>19,}")
    
    if total != unique_wsr:
        print(f"      {'[WARNING] Duplicates':<30} {total - unique_wsr:>19,}")
    
    print(f"\n      NULL Value Counts:")
    for col in ['WSR_ID', 'EMPLOYER_ID', 'DATE_RECEIVED', 'DAYS', 'WAGES', 
                'CHARGES', 'INTEREST', 'INVOICE_AMOUNT', 'DUE_DATE']:
        null_col = f'NULL_{col}'
        if null_col in df_quality.columns:
            null_count = df_quality[null_col].iloc[0]
            pct = (null_count / total * 100) if total > 0 else 0
            print(f"      {col:<30} {null_count:>19,} {pct:>14.1f}%")
    
    print(f"\n      Value Ranges:")
    print(f"      {'Field':<30} {'Min':<20} {'Max':<20}")
    print(f"      {'-'*30} {'-'*20} {'-'*20}")
    print(f"      {'Days':<30} {df_quality['MIN_DAYS'].iloc[0] if pd.notna(df_quality['MIN_DAYS'].iloc[0]) else 'NULL':>19} {df_quality['MAX_DAYS'].iloc[0] if pd.notna(df_quality['MAX_DAYS'].iloc[0]) else 'NULL':>19}")
    print(f"      {'Wages':<30} {df_quality['MIN_WAGES'].iloc[0] if pd.notna(df_quality['MIN_WAGES'].iloc[0]) else 'NULL':>19,.2f} {df_quality['MAX_WAGES'].iloc[0] if pd.notna(df_quality['MAX_WAGES'].iloc[0]) else 'NULL':>19,.2f}")
    print(f"      {'Charges':<30} {df_quality['MIN_CHARGES'].iloc[0] if pd.notna(df_quality['MIN_CHARGES'].iloc[0]) else 'NULL':>19} {df_quality['MAX_CHARGES'].iloc[0] if pd.notna(df_quality['MAX_CHARGES'].iloc[0]) else 'NULL':>19}")
    
except Exception as e:
    print(f"      [ERROR] {e}")

conn.close()

print("\n" + "="*70)
print("Salesforce Field Type Requirements:")
print("="*70)
print("""
Return__c Field Mappings:
  External_Id__c           Text              ← WSR_ID (NUMBER)               str(int())
  Employer__c              Lookup(Account)   ← EMPLOYER_ID (NUMBER)         via External_Id__c
  ReturnSubmittedDate__c   Date              ← DATE_RECEIVED (DATE)         strftime('%Y-%m-%d')
  TotalDaysWorked__c       Number(18,0)      ← EMPLOYER_DAYS (NUMBER)       int()
  TotalDaysReported__c     Number(18,0)      ← EMPLOYER_DAYS (NUMBER)       int()
  TotalWagesReported__c    Currency(16,2)    ← EMPLOYER_TOTAL_WAGES (NUM)   float()
  Charges__c               Currency(16,2)    ← CONTRIBUTION_AMOUNT (NUM)    float()
  Interest__c              Currency(12,2)    ← STATUATORY_INTEREST (NUM)   float()
  InvoiceAmount__c         Currency(12,2)    ← AMOUNT (NUMBER)              float()
  AmountPayable__c         Currency(12,2)    ← AMOUNT (NUMBER)              float()
  InvoiceDueDate__c        Date              ← PAYMENT_DUE_DATE (DATE)      strftime('%Y-%m-%d')
  ReturnType__c            Picklist          ← EVENT_TYPE_CODE (code)       CO_CODE lookup
  InvoiceStatus__c         Picklist          ← STATUS_CODE (code)           CO_CODE lookup

Potential Issues:
  1. If WSR_ID has duplicates, upsert will update the same record multiple times
  2. Currency fields should check for overflow (SF max: 999,999,999,999.99)
  3. Number(18,0) can hold max: 999,999,999,999,999,999
""")

print("\n[COMPLETE]")
