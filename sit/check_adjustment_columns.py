import os
from dotenv import load_dotenv
import oracledb

load_dotenv('.env.sit')

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER='SCH_CO_20' 
    AND TABLE_NAME='CO_ADJUSTMENT' 
    ORDER BY COLUMN_ID
""")

print("CO_ADJUSTMENT columns:")
for row in cursor.fetchall():
    print(f"  {row[0]:<40} {row[1]}")

cursor.close()
conn.close()
