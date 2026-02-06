"""
Test ABN join between Oracle and SQL Server to find format mismatch
"""

import os
from dotenv import load_dotenv
import oracledb
import pyodbc
import pandas as pd

load_dotenv('.env.sit')

print("\n" + "="*80)
print("ABN JOIN TEST - Oracle vs SQL Server")
print("="*80)

# Get sample Oracle ABNs
print("\n[1] Oracle ABN Sample (first 10 records):")
print("-" * 80)

oracle_conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = oracle_conn.cursor()
cursor.execute("""
    SELECT CUSTOMER_ID, ABN
    FROM SCH_CO_20.CO_EMPLOYER
    WHERE ABN IS NOT NULL
    AND ROWNUM <= 10
    ORDER BY CUSTOMER_ID
""")

oracle_abns = []
for row in cursor.fetchall():
    customer_id, abn = row
    print(f"  CUSTOMER_ID: {customer_id:8d}  ABN: {abn} (type: {type(abn).__name__})")
    oracle_abns.append(str(int(abn)))

oracle_conn.close()

# Get sample SQL Server ABNs
print("\n[2] SQL Server ABN Sample (first 10 records):")
print("-" * 80)

sql_conn_str = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER=cosql-test.coinvest.com.au;'
    f'DATABASE=AvatarWarehouse;'
    f'Trusted_Connection=yes;'
)

sql_conn = pyodbc.connect(sql_conn_str)
cursor = sql_conn.cursor()
cursor.execute("""
    SELECT TOP 10 [Australian Business Number]
    FROM [datascience].[abr_cleaned]
    WHERE [Australian Business Number] IS NOT NULL
    ORDER BY [Australian Business Number]
""")

sql_abns = []
for row in cursor.fetchall():
    abn = row[0]
    print(f"  ABN: {abn} (type: {type(abn).__name__})")
    sql_abns.append(str(abn))

sql_conn.close()

# Test join
print("\n[3] Testing Join:")
print("-" * 80)
print(f"Oracle ABN sample (cleaned): {oracle_abns[:3]}")
print(f"SQL ABN sample (cleaned): {sql_abns[:3]}")

# Check if any Oracle ABNs exist in SQL Server
matches = set(oracle_abns) & set(sql_abns)
print(f"\nMatches found: {len(matches)}")
if matches:
    print(f"Sample matches: {list(matches)[:5]}")

# Get full count comparison
print("\n[4] Full Count Comparison:")
print("-" * 80)

oracle_conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = oracle_conn.cursor()
cursor.execute("SELECT COUNT(*), COUNT(ABN) FROM SCH_CO_20.CO_EMPLOYER WHERE ABN IS NOT NULL")
result = cursor.fetchone()
print(f"Oracle: {result[0]:,} records with ABN")

# Get a specific Oracle ABN to test
cursor.execute("SELECT CUSTOMER_ID, ABN FROM SCH_CO_20.CO_EMPLOYER WHERE ABN = 11001035054 AND ROWNUM <= 1")
test_result = cursor.fetchone()
if test_result:
    print(f"\nOracle test record: CUSTOMER_ID={test_result[0]}, ABN={test_result[1]}")
    test_abn = str(int(test_result[1]))
    print(f"Cleaned ABN: {test_abn}")
    
    # Check if this exists in SQL Server
    sql_conn = pyodbc.connect(sql_conn_str)
    cursor = sql_conn.cursor()
    cursor.execute(f"SELECT [Australian Business Number] FROM [datascience].[abr_cleaned] WHERE [Australian Business Number] = {test_abn}")
    sql_result = cursor.fetchone()
    if sql_result:
        print(f"✅ Found in SQL Server: {sql_result[0]}")
    else:
        print(f"❌ NOT found in SQL Server")
    sql_conn.close()

oracle_conn.close()

sql_conn = pyodbc.connect(sql_conn_str)
cursor = sql_conn.cursor()
cursor.execute("SELECT COUNT(*) FROM [datascience].[abr_cleaned] WHERE [Australian Business Number] IS NOT NULL")
result = cursor.fetchone()
print(f"SQL Server: {result[0]:,} records with Australian Business Number")
sql_conn.close()

print("\n" + "="*80)
print("RECOMMENDATION:")
print("  If join found 0 matches, check:")
print("  1. ABN data type conversion (NUMBER → string)")
print("  2. Leading zeros or padding differences")
print("  3. SQL Server might use different ABN format")
print("="*80)
