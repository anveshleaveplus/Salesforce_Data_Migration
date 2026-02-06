"""
Check why ABR match count differs between load script (30,572) and Salesforce (27,624)
"""
import os
import pandas as pd
import pyodbc
import oracledb
from dotenv import load_dotenv

load_dotenv('.env.sit')

print("\n" + "="*70)
print("Investigating ABR Match Count Discrepancy")
print("="*70)
print("\nLoad script reported: 30,572 matched")
print("Salesforce shows:     27,624 with ABR data")
print("Difference:           2,948 records\n")

# Connect to Oracle
oracle_conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    dsn=os.getenv('ORACLE_DSN')
)

# Extract Oracle data
oracle_query = """
SELECT 
    e.CUSTOMER_ID as CUSTOMER_ID,
    e.CUSTOMER_ID_REG as CUSTOMER_ID_REG,
    e.ABN,
    e.ACN,
    e.TRADING_NAME,
    e.TRADING_NAME_REGISTERED,
    e.TRADING_NAME_AS,
    e.EMPLOYMENT_START_DATE
FROM SCH_CO_20.CO_EMPLOYER e
WHERE EXISTS (
    SELECT 1 FROM SCH_CO_20.CO_WSR_SERVICE s
    WHERE s.CUSTOMER_ID = e.CUSTOMER_ID
    AND s.SERVICE >= 202301
)
"""

print("Extracting Oracle data...")
df_oracle = pd.read_sql(oracle_query, oracle_conn)
oracle_conn.close()
print(f"  Oracle extracted: {len(df_oracle):,} records")

# Connect to SQL Server
sql_conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=cosql-test.coinvest.com.au;'
    'DATABASE=AvatarWarehouse;'
    'Trusted_Connection=yes;'
)

# Extract SQL Server ABR data
sql_query = """
SELECT 
    CAST([Australian Business Number] AS VARCHAR(20)) as ABN,
    [ABN Registration - Date of Effect] as ABN_Registration_Date,
    [ABN Status] as ABN_Status,
    [Main - Industry Class Code] as Industry_Class_Code
FROM datascience.abr_cleaned
"""

print("Extracting SQL Server ABR data...")
cursor = sql_conn.cursor()
cursor.execute(sql_query)
abr_records = {}
for row in cursor.fetchall():
    abn = str(row[0]).strip() if row[0] else None
    if abn:
        abr_records[abn] = {
            'ABN_Registration_Date': row[1],
            'ABN_Status': row[2],
            'Industry_Class_Code': row[3]
        }
sql_conn.close()
print(f"  SQL Server extracted: {len(abr_records):,} ABR records")

# Clean Oracle ABN and check matches
df_oracle['ABN_CLEAN'] = df_oracle['ABN'].apply(
    lambda x: str(int(x)).strip() if pd.notna(x) and x != 0 else None
)

# Count matches before deduplication
matched_before_dedup = df_oracle[df_oracle['ABN_CLEAN'].isin(abr_records.keys())]
print(f"\n  Matched before deduplication: {len(matched_before_dedup):,}")

# Now check for duplicates
duplicates = df_oracle[df_oracle.duplicated(subset=['CUSTOMER_ID'], keep=False)]
print(f"  Duplicate CUSTOMER_IDs: {len(duplicates):,} records")

# Deduplicate (keep first)
df_oracle_dedup = df_oracle.drop_duplicates(subset=['CUSTOMER_ID'], keep='first')
print(f"  After deduplication: {len(df_oracle_dedup):,} records")

# Count matches after deduplication
matched_after_dedup = df_oracle_dedup[df_oracle_dedup['ABN_CLEAN'].isin(abr_records.keys())]
print(f"  Matched after deduplication: {len(matched_after_dedup):,}")

# Now merge and check for NULL values in SQL Server fields
df_oracle_dedup['ABN_Registration_Date'] = df_oracle_dedup['ABN_CLEAN'].map(
    lambda x: abr_records.get(x, {}).get('ABN_Registration_Date') if x else None
)
df_oracle_dedup['ABN_Status'] = df_oracle_dedup['ABN_CLEAN'].map(
    lambda x: abr_records.get(x, {}).get('ABN_Status') if x else None
)
df_oracle_dedup['Industry_Class_Code'] = df_oracle_dedup['ABN_CLEAN'].map(
    lambda x: abr_records.get(x, {}).get('Industry_Class_Code') if x else None
)

# Count how many have non-NULL values
has_reg_date = df_oracle_dedup['ABN_Registration_Date'].notna().sum()
has_status = df_oracle_dedup['ABN_Status'].notna().sum()
has_code = df_oracle_dedup['Industry_Class_Code'].notna().sum()

print(f"\n  Records with ABN_Registration_Date: {has_reg_date:,}")
print(f"  Records with ABN_Status: {has_status:,}")
print(f"  Records with Industry_Class_Code: {has_code:,}")

# Find records that matched but have NULL SQL Server data
matched_but_null = df_oracle_dedup[
    (df_oracle_dedup['ABN_CLEAN'].isin(abr_records.keys())) &
    (df_oracle_dedup['ABN_Registration_Date'].isna())
]
print(f"\n  Matched ABN but NULL SQL Server data: {len(matched_but_null):,}")

# Show samples
if len(matched_but_null) > 0:
    print("\n  Sample records with matched ABN but NULL SQL data:")
    for idx, row in matched_but_null.head(3).iterrows():
        print(f"    ABN: {row['ABN_CLEAN']}, SQL record: {abr_records.get(row['ABN_CLEAN'])}")

print("\n" + "="*70)
print("Summary of Discrepancy")
print("="*70)
print(f"Before deduplication matched:  30,572")
print(f"After deduplication matched:   {len(matched_after_dedup):,}")
print(f"With non-NULL SQL Server data: {has_reg_date:,}")
print(f"Salesforce verification shows: 27,624")
print(f"\nLikely explanation:")
print(f"  - Deduplication removed some matched records")
print(f"  - Some matched ABNs have NULL values in SQL Server")
print("="*70)
