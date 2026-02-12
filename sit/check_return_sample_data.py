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

# Check what data exists in first batch
cursor.execute("""
    SELECT 
        wsr.WSR_ID,
        wsr.EMPLOYER_ID,
        wsr.EVENT_TYPE_CODE,
        inv_det.STATUS_CODE
    FROM SCH_CO_20.CO_WSR wsr
    LEFT JOIN SCH_CO_20.CO_ACC_INVOICE inv 
        ON inv.CUSTOMER_ID = wsr.EMPLOYER_ID 
        AND inv.WSR_PERIOD = wsr.PERIOD_END
    LEFT JOIN SCH_CO_20.CO_ACC_INVOICE_DETAIL inv_det 
        ON inv_det.INVOICE_ID = inv.INVOICE_ID
    WHERE wsr.PERIOD_END >= 202301
    AND wsr.EMPLOYER_ID != 23000
    AND ROWNUM <= 10
""")

print("Sample Return data (first 10 records):")
print(f"{'WSR_ID':<12} {'EMPLOYER_ID':<15} {'EVENT_TYPE':<15} {'STATUS_CODE'}")
print("-" * 60)
for row in cursor.fetchall():
    print(f"{row[0]:<12} {row[1]:<15} {str(row[2]):<15} {row[3]}")

cursor.close()
conn.close()
