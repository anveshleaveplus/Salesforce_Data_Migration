"""
Check data types for Account mapping - Source vs Destination
Sources: Oracle CO_EMPLOYER, SQL Server abr_cleaned
Destination: Salesforce Account object
"""

import os
from dotenv import load_dotenv
import oracledb
import pyodbc
from simple_salesforce import Salesforce

load_dotenv('.env.sit')

print("\n" + "="*80)
print("DATA TYPE VERIFICATION - ACCOUNT MAPPING")
print("="*80)

# ============================================================================
# 1. ORACLE DATA TYPES
# ============================================================================
print("\n[1/3] ORACLE DATA TYPES (SCH_CO_20)")
print("-" * 80)

try:
    conn = oracledb.connect(
        user=os.getenv('ORACLE_USER'),
        password=os.getenv('ORACLE_PASSWORD'),
        host=os.getenv('ORACLE_HOST'),
        port=int(os.getenv('ORACLE_PORT')),
        sid=os.getenv('ORACLE_SID')
    )
    cursor = conn.cursor()
    
    oracle_fields = [
        ('CO_EMPLOYER', 'CUSTOMER_ID'),
        ('CO_EMPLOYER', 'ABN'),
        ('CO_EMPLOYER', 'ACN'),
        ('CO_EMPLOYER', 'TRADING_NAME'),
        ('CO_WSR_SERVICE', 'EMPLOYMENT_START_DATE'),
    ]
    
    for table, column in oracle_fields:
        cursor.execute(f"""
            SELECT data_type, data_length, data_precision, data_scale
            FROM all_tab_columns
            WHERE table_name = '{table}'
            AND owner = 'SCH_CO_20'
            AND column_name = '{column}'
        """)
        result = cursor.fetchone()
        if result:
            dtype, length, precision, scale = result
            type_info = f"{dtype}"
            if precision:
                type_info += f"({precision}"
                if scale:
                    type_info += f",{scale}"
                type_info += ")"
            elif length:
                type_info += f"({length})"
            
            print(f"  {table}.{column:30s} => {type_info}")
        else:
            print(f"  {table}.{column:30s} => NOT FOUND")
    
    cursor.close()
    conn.close()
    print("  ✅ Oracle check complete")
    
except Exception as e:
    print(f"  ❌ Oracle error: {e}")

# ============================================================================
# 2. SQL SERVER DATA TYPES
# ============================================================================
print("\n[2/3] SQL SERVER DATA TYPES (AvatarWarehouse.datascience.abr_cleaned)")
print("-" * 80)

try:
    sql_server = 'cosql-test.coinvest.com.au'
    database = 'AvatarWarehouse'
    
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={sql_server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    sql_fields = [
        'ABN',
        '[ABN Registration - Date of Effect]',
        '[ABN Status]',
        '[Main - Industry Class]',
        '[Main - Industry Class Code]'
    ]
    
    query = """
    SELECT 
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'datascience'
    AND TABLE_NAME = 'abr_cleaned'
    AND COLUMN_NAME IN (
        'ABN',
        'ABN Registration - Date of Effect',
        'ABN Status',
        'Main - Industry Class',
        'Main - Industry Class Code'
    )
    """
    
    cursor.execute(query)
    for row in cursor.fetchall():
        col_name, dtype, char_len, num_precision, num_scale = row
        type_info = f"{dtype}"
        if char_len:
            type_info += f"({char_len})"
        elif num_precision:
            type_info += f"({num_precision}"
            if num_scale:
                type_info += f",{num_scale}"
            type_info += ")"
        
        print(f"  {col_name:35s} => {type_info}")
    
    cursor.close()
    conn.close()
    print("  ✅ SQL Server check complete")
    
except Exception as e:
    print(f"  ❌ SQL Server error: {e}")

# ============================================================================
# 3. SALESFORCE DATA TYPES
# ============================================================================
print("\n[3/3] SALESFORCE DATA TYPES (Account Object)")
print("-" * 80)

try:
    sf = Salesforce(
        username=os.getenv('SF_USERNAME'),
        password=os.getenv('SF_PASSWORD'),
        security_token=os.getenv('SF_SECURITY_TOKEN'),
        domain='test'
    )
    
    account_desc = sf.Account.describe()
    
    sf_fields = [
        'External_Id__c',
        'Registration_Number__c',
        'ABN__c',
        'ACN__c',
        'Name',
        'RegisteredEntityName__c',
        'TradingAs__c',
        'DateEmploymentCommenced__c',
        'ABNRegistrationDate__c',
        'AccountStatus__c',
        'Classifications__c',
        'OSCACode__c'
    ]
    
    for field_name in sf_fields:
        field = next((f for f in account_desc['fields'] if f['name'] == field_name), None)
        if field:
            type_info = field['type']
            if field['length'] and field['length'] > 0:
                type_info += f"({field['length']})"
            
            required = "REQUIRED" if not field['nillable'] else "Optional"
            external_id = " [EXTERNAL ID]" if field.get('externalId') else ""
            
            print(f"  {field_name:35s} => {type_info:20s} {required}{external_id}")
        else:
            print(f"  {field_name:35s} => ❌ NOT FOUND")
    
    print("  ✅ Salesforce check complete")
    
except Exception as e:
    print(f"  ❌ Salesforce error: {e}")

# ============================================================================
# 4. MAPPING SUMMARY
# ============================================================================
print("\n" + "="*80)
print("MAPPING SUMMARY")
print("="*80)

mappings = [
    ("Oracle: CO_EMPLOYER.CUSTOMER_ID (NUMBER)", "SF: External_Id__c (Text)", "✅ NUMBER → Text"),
    ("Oracle: CO_EMPLOYER.ABN (VARCHAR2)", "SF: ABN__c (Text)", "✅ VARCHAR2 → Text"),
    ("Oracle: CO_EMPLOYER.ACN (VARCHAR2)", "SF: ACN__c (Text)", "✅ VARCHAR2 → Text"),
    ("Oracle: CO_EMPLOYER.TRADING_NAME (VARCHAR2)", "SF: Name (Text)", "✅ VARCHAR2 → Text"),
    ("Oracle: CO_EMPLOYER.TRADING_NAME (VARCHAR2)", "SF: RegisteredEntityName__c (Text)", "✅ VARCHAR2 → Text"),
    ("Oracle: CO_EMPLOYER.TRADING_NAME (VARCHAR2)", "SF: TradingAs__c (Text)", "✅ VARCHAR2 → Text"),
    ("Oracle: CO_WSR_SERVICE.EMPLOYMENT_START_DATE (DATE)", "SF: DateEmploymentCommenced__c (Date)", "✅ DATE → Date"),
    ("SQL: abr_cleaned.ABN", "SF: ABN__c (Text)", "ℹ️ Join key"),
    ("SQL: abr_cleaned.[ABN Registration - Date of Effect]", "SF: ABNRegistrationDate__c (Date)", "⚠️ Check SQL type"),
    ("SQL: abr_cleaned.[ABN Status]", "SF: AccountStatus__c (Picklist)", "⚠️ VARCHAR → Picklist"),
    ("SQL: abr_cleaned.[Main - Industry Class]", "SF: Classifications__c (Picklist)", "⚠️ VARCHAR → Picklist"),
    ("SQL: abr_cleaned.[Main - Industry Class Code]", "SF: OSCACode__c (Text)", "⚠️ Check SQL type"),
]

print("\nSource → Destination Compatibility:")
for source, dest, status in mappings:
    print(f"  {status}  {source:55s} → {dest}")

print("\n" + "="*80)
print("⚠️  WARNINGS:")
print("  1. Picklist fields require value validation (AccountStatus__c, Classifications__c)")
print("  2. Verify SQL Server date format matches Salesforce date format")
print("  3. Check if OSCACode__c length is sufficient for Industry Class Code")
print("="*80)
