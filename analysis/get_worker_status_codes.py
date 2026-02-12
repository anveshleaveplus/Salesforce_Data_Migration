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

print("Worker Status Code Mappings (Code Set 12)")
print("="*60)

cursor = conn.cursor()
cursor.execute("""
    SELECT VALUE, DESCRIPTION
    FROM SCH_CO_20.CO_CODE
    WHERE CODE_SET_ID = 12
    ORDER BY VALUE
""")

for value, description in cursor:
    print(f"  {value} -> {description}")

conn.close()
