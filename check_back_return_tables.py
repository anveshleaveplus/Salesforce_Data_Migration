from dotenv import load_dotenv
import os
import oracledb
import pandas as pd

load_dotenv()

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

# Check CO_BACK_RETURN_REQUEST table structure
print("CO_BACK_RETURN_REQUEST table structure:")
print("="*80)
query = """
SELECT column_name, data_type, data_length, nullable
FROM all_tab_columns
WHERE owner = 'SCH_CO_20' AND table_name = 'CO_BACK_RETURN_REQUEST'
ORDER BY column_id
"""
df = pd.read_sql(query, conn)
print(df.to_string(index=False))

# Sample data
print("\n\nSample data from CO_BACK_RETURN_REQUEST:")
print("="*80)
query = """
SELECT * FROM SCH_CO_20.CO_BACK_RETURN_REQUEST
WHERE ROWNUM <= 5
"""
df_sample = pd.read_sql(query, conn)
print(df_sample.to_string(index=False))

# Check CO_BACK_RETURN_PERIOD table
print("\n\nCO_BACK_RETURN_PERIOD table structure:")
print("="*80)
query = """
SELECT column_name, data_type, data_length, nullable
FROM all_tab_columns
WHERE owner = 'SCH_CO_20' AND table_name = 'CO_BACK_RETURN_PERIOD'
ORDER BY column_id
"""
df = pd.read_sql(query, conn)
print(df.to_string(index=False))

# Sample data
print("\n\nSample data from CO_BACK_RETURN_PERIOD:")
print("="*80)
query = """
SELECT * FROM SCH_CO_20.CO_BACK_RETURN_PERIOD
WHERE ROWNUM <= 5
"""
df_sample = pd.read_sql(query, conn)
print(df_sample.to_string(index=False))

# Check how these relate to employers
print("\n\nCount of back returns by employer:")
print("="*80)
query = """
SELECT COUNT(*) as total_requests,
       COUNT(DISTINCT CUSTOMER_ID) as unique_employers
FROM SCH_CO_20.CO_BACK_RETURN_REQUEST
"""
df = pd.read_sql(query, conn)
print(df.to_string(index=False))

conn.close()
