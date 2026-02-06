from dotenv import load_dotenv
import os
import oracledb

load_dotenv()

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

# Search for columns with 'return' in the name
query = """
SELECT table_name, column_name, data_type
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
AND (LOWER(column_name) LIKE '%return%' 
     OR LOWER(column_name) LIKE '%year%'
     OR LOWER(column_name) LIKE '%annual%'
     OR LOWER(column_name) LIKE '%last%')
ORDER BY table_name, column_name
"""

cursor = conn.cursor()
cursor.execute(query)
results = cursor.fetchall()

print("Oracle Columns with 'return', 'year', 'annual', or 'last':")
print("="*80)

current_table = None
for table, column, dtype in results:
    if table != current_table:
        print(f"\n{table}:")
        current_table = table
    print(f"  - {column} ({dtype})")

print(f"\nTotal columns found: {len(results)}")

conn.close()
