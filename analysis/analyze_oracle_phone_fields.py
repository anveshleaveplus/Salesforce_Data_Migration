"""
Analyze TELEPHONE1_NO and TELEPHONE2_NO in production Oracle to determine phone type
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
print("ORACLE TELEPHONE FIELD ANALYSIS - PRODUCTION DATA")
print("="*80)

# 1. Check column metadata/comments
print("\n[1] Column Metadata from Oracle Data Dictionary")
print("-"*80)

metadata_query = """
SELECT 
    column_name,
    data_type,
    data_length,
    nullable,
    comments
FROM all_tab_columns atc
LEFT JOIN all_col_comments acc 
    ON atc.owner = acc.owner 
    AND atc.table_name = acc.table_name 
    AND atc.column_name = acc.column_name
WHERE atc.owner = 'SCH_CO_20'
    AND atc.table_name = 'CO_CUSTOMER'
    AND atc.column_name IN ('MOBILE_PHONE_NO', 'TELEPHONE1_NO', 'TELEPHONE2_NO')
ORDER BY atc.column_name
"""

try:
    cursor.execute(metadata_query)
    metadata = cursor.fetchall()
    
    print(f"{'COLUMN':<20} {'TYPE':<15} {'LENGTH':<10} {'NULLABLE':<10} {'COMMENTS':<30}")
    print("-"*80)
    for row in metadata:
        col = row[0] if row[0] else ''
        dtype = row[1] if row[1] else ''
        length = row[2] if row[2] else ''
        nullable = row[3] if row[3] else ''
        comments = row[4] if row[4] else 'No comments'
        print(f"{col:<20} {dtype:<15} {str(length):<10} {nullable:<10} {str(comments)[:30]:<30}")
except Exception as e:
    print(f"Could not retrieve metadata: {e}")

# 2. Get sample data from ALL workers (not just active)
print("\n[2] Sample Data from CO_CUSTOMER (20 random records with phone numbers)")
print("-"*80)

sample_query = """
SELECT 
    c.CUSTOMER_ID,
    c.MOBILE_PHONE_NO,
    c.TELEPHONE1_NO,
    c.TELEPHONE2_NO
FROM SCH_CO_20.CO_CUSTOMER c
WHERE (c.TELEPHONE1_NO IS NOT NULL OR c.TELEPHONE2_NO IS NOT NULL OR c.MOBILE_PHONE_NO IS NOT NULL)
    AND ROWNUM <= 20
ORDER BY c.CUSTOMER_ID
"""

cursor.execute(sample_query)
samples = cursor.fetchall()

print(f"{'CUSTOMER_ID':<12} {'MOBILE_PHONE_NO':<18} {'TELEPHONE1_NO':<18} {'TELEPHONE2_NO':<18}")
print("-"*80)
for row in samples:
    cid = str(row[0]) if row[0] else 'NULL'
    mobile = str(row[1])[:17] if row[1] else 'NULL'
    tel1 = str(row[2])[:17] if row[2] else 'NULL'
    tel2 = str(row[3])[:17] if row[3] else 'NULL'
    print(f"{cid:<12} {mobile:<18} {tel1:<18} {tel2:<18}")

# 3. Check phone number patterns (mobile vs landline indicators)
print("\n[3] Phone Number Pattern Analysis")
print("-"*80)

# Australian phone patterns:
# Mobile typically start with 04 (e.g., 0412 345 678)
# Landline typically start with 02, 03, 07, 08 (e.g., 02 9876 5432)

pattern_query = """
SELECT 
    'MOBILE_PHONE_NO' as field_name,
    COUNT(*) as total_count,
    SUM(CASE WHEN MOBILE_PHONE_NO LIKE '04%' THEN 1 ELSE 0 END) as starts_04_count,
    SUM(CASE WHEN MOBILE_PHONE_NO LIKE '02%' OR MOBILE_PHONE_NO LIKE '03%' 
             OR MOBILE_PHONE_NO LIKE '07%' OR MOBILE_PHONE_NO LIKE '08%' THEN 1 ELSE 0 END) as landline_pattern
FROM SCH_CO_20.CO_CUSTOMER
WHERE MOBILE_PHONE_NO IS NOT NULL
UNION ALL
SELECT 
    'TELEPHONE1_NO' as field_name,
    COUNT(*) as total_count,
    SUM(CASE WHEN TELEPHONE1_NO LIKE '04%' THEN 1 ELSE 0 END) as starts_04_count,
    SUM(CASE WHEN TELEPHONE1_NO LIKE '02%' OR TELEPHONE1_NO LIKE '03%' 
             OR TELEPHONE1_NO LIKE '07%' OR TELEPHONE1_NO LIKE '08%' THEN 1 ELSE 0 END) as landline_pattern
FROM SCH_CO_20.CO_CUSTOMER
WHERE TELEPHONE1_NO IS NOT NULL
UNION ALL
SELECT 
    'TELEPHONE2_NO' as field_name,
    COUNT(*) as total_count,
    SUM(CASE WHEN TELEPHONE2_NO LIKE '04%' THEN 1 ELSE 0 END) as starts_04_count,
    SUM(CASE WHEN TELEPHONE2_NO LIKE '02%' OR TELEPHONE2_NO LIKE '03%' 
             OR TELEPHONE2_NO LIKE '07%' OR TELEPHONE2_NO LIKE '08%' THEN 1 ELSE 0 END) as landline_pattern
FROM SCH_CO_20.CO_CUSTOMER
WHERE TELEPHONE2_NO IS NOT NULL
"""

cursor.execute(pattern_query)
patterns = cursor.fetchall()

print(f"{'FIELD':<20} {'TOTAL':<12} {'MOBILE (04%)':<15} {'LANDLINE (02/03/07/08)':<25}")
print("-"*80)
for row in patterns:
    field = row[0]
    total = row[1]
    mobile_pct = (row[2]/total*100) if total > 0 else 0
    landline_pct = (row[3]/total*100) if total > 0 else 0
    print(f"{field:<20} {total:<12,} {row[2]:>6,} ({mobile_pct:>5.1f}%)   {row[3]:>6,} ({landline_pct:>5.1f}%)")

# 4. Coverage statistics
print("\n[4] Overall Coverage Statistics")
print("-"*80)

coverage_query = """
SELECT 
    COUNT(*) as total_customers,
    SUM(CASE WHEN MOBILE_PHONE_NO IS NOT NULL THEN 1 ELSE 0 END) as has_mobile,
    SUM(CASE WHEN TELEPHONE1_NO IS NOT NULL THEN 1 ELSE 0 END) as has_tel1,
    SUM(CASE WHEN TELEPHONE2_NO IS NOT NULL THEN 1 ELSE 0 END) as has_tel2,
    SUM(CASE WHEN TELEPHONE1_NO IS NOT NULL AND TELEPHONE2_NO IS NOT NULL THEN 1 ELSE 0 END) as has_both_tel
FROM SCH_CO_20.CO_CUSTOMER
"""

cursor.execute(coverage_query)
stats = cursor.fetchone()

total = stats[0]
print(f"Total Customers:     {total:,}")
print(f"Has MOBILE_PHONE_NO: {stats[1]:,} ({stats[1]/total*100:.1f}%)")
print(f"Has TELEPHONE1_NO:   {stats[2]:,} ({stats[2]/total*100:.1f}%)")
print(f"Has TELEPHONE2_NO:   {stats[3]:,} ({stats[3]/total*100:.1f}%)")
print(f"Has Both TEL1 & TEL2: {stats[4]:,} ({stats[4]/total*100:.1f}%)")

print("\n" + "="*80)
print("MAPPING RECOMMENDATION - Based on data patterns")
print("="*80)

cursor.close()
conn.close()
