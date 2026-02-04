import oracledb
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

# Check CO_WSR_SERVICE table for return dates
print("CO_WSR_SERVICE columns with 'date' or 'period':")
print("="*80)
cursor = conn.cursor()
cursor.execute("""
    SELECT column_name, data_type 
    FROM all_tab_columns 
    WHERE owner = 'SCH_CO_20' 
    AND table_name = 'CO_WSR_SERVICE' 
    AND (LOWER(column_name) LIKE '%date%' 
         OR LOWER(column_name) LIKE '%period%'
         OR LOWER(column_name) LIKE '%return%'
         OR LOWER(column_name) LIKE '%submit%')
    ORDER BY column_name
""")
for row in cursor.fetchall():
    print(f'  {row[0]:<40} {row[1]}')

# Check all columns in CO_WSR_SERVICE
print("\n\nAll columns in CO_WSR_SERVICE:")
print("="*80)
cursor.execute("""
    SELECT column_name, data_type 
    FROM all_tab_columns 
    WHERE owner = 'SCH_CO_20' 
    AND table_name = 'CO_WSR_SERVICE' 
    ORDER BY column_id
""")
for row in cursor.fetchall():
    print(f'  {row[0]:<40} {row[1]}')

# Look for return-related tables
print("\n\nTables with 'RETURN' or 'SERVICE' in name:")
print("="*80)
cursor.execute("""
    SELECT table_name 
    FROM all_tables 
    WHERE owner = 'SCH_CO_20' 
    AND (table_name LIKE '%RETURN%' OR table_name LIKE '%SERVICE%')
    AND table_name NOT LIKE '%JN'
    AND table_name NOT LIKE '%TMP%'
    ORDER BY table_name
""")
for row in cursor.fetchall():
    print(f'  {row[0]}')

# Check CO_CONSOLIDATED_SERVICE table
print("\n\nCO_CONSOLIDATED_SERVICE columns:")
print("="*80)
cursor.execute("""
    SELECT column_name, data_type 
    FROM all_tab_columns 
    WHERE owner = 'SCH_CO_20' 
    AND table_name = 'CO_CONSOLIDATED_SERVICE' 
    ORDER BY column_id
""")
for row in cursor.fetchall():
    print(f'  {row[0]:<40} {row[1]}')

conn.close()
