"""
Check if Oracle has HOME_PHONE or similar phone columns in CO_CUSTOMER
"""
import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

# Oracle connection
conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print("="*80)
print("ORACLE CO_CUSTOMER TABLE - ALL PHONE COLUMNS")
print("="*80)

# Get all columns containing 'PHONE' or 'TEL'
query = """
SELECT 
    column_name,
    data_type,
    data_length,
    nullable
FROM all_tab_columns
WHERE owner = 'SCH_CO_20'
    AND table_name = 'CO_CUSTOMER'
    AND (column_name LIKE '%PHONE%' OR column_name LIKE '%TEL%')
ORDER BY column_name
"""

cursor.execute(query)
columns = cursor.fetchall()

print(f"\nAll phone/telephone columns found:")
print("-"*80)
print(f"{'COLUMN_NAME':<30} {'DATA_TYPE':<15} {'LENGTH':<10} {'NULLABLE':<10}")
print("-"*80)

for row in columns:
    print(f"{row[0]:<30} {row[1]:<15} {str(row[2]):<10} {row[3]:<10}")

print(f"\nTotal phone-related columns: {len(columns)}")

# Check if there's a HOME_PHONE column
has_home_phone = any('HOME' in col[0] for col in columns)
has_work_phone = any('WORK' in col[0] or 'BUSINESS' in col[0] for col in columns)

print("\n" + "="*80)
print("ANALYSIS:")
print("-"*80)
print(f"Has HOME_PHONE column? {'YES ✓' if has_home_phone else 'NO ✗'}")
print(f"Has WORK/BUSINESS_PHONE column? {'YES ✓' if has_work_phone else 'NO ✗'}")
print("\nColumns found:")
for col in columns:
    print(f"  - {col[0]}")

if not has_home_phone:
    print("\n⚠️  No HOME_PHONE column found in CO_CUSTOMER")
    print("📌 TELEPHONE1_NO and TELEPHONE2_NO don't have specific type indicators")
    print("📌 Cannot determine from column names if they're home/work/business phones")

print("="*80)

cursor.close()
conn.close()
