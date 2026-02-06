import oracledb
from dotenv import load_dotenv
import os

load_dotenv()

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()
cursor.execute("""
    SELECT column_name, data_type 
    FROM all_tab_columns 
    WHERE owner = 'SCH_CO_20' 
    AND table_name = 'CO_EMPLOYER' 
    AND (LOWER(column_name) LIKE '%regist%' 
         OR LOWER(column_name) LIKE '%date%' 
         OR LOWER(column_name) LIKE '%creat%')
    ORDER BY column_name
""")

print('Registration/Date fields on CO_EMPLOYER:')
print('='*60)
for row in cursor.fetchall():
    print(f'  {row[0]:<40} {row[1]}')

# Check sample values
print('\n\nSample FIRST_EMPLOYED_WORKERS_DATE:')
print('='*60)
cursor.execute("""
    SELECT CUSTOMER_ID, TRADING_NAME, FIRST_EMPLOYED_WORKERS_DATE
    FROM SCH_CO_20.CO_EMPLOYER 
    WHERE ROWNUM <= 10
    AND FIRST_EMPLOYED_WORKERS_DATE IS NOT NULL
    ORDER BY FIRST_EMPLOYED_WORKERS_DATE DESC
""")
for row in cursor.fetchall():
    name = row[1][:40] if row[1] else 'N/A'
    print(f'  {row[0]}: {name:<40} First Employed: {row[2]}')

# Count by year
print('\n\nEmployer count by first employment year:')
print('='*60)
cursor.execute("""
    SELECT EXTRACT(YEAR FROM FIRST_EMPLOYED_WORKERS_DATE) as year, 
           COUNT(*) as employer_count
    FROM SCH_CO_20.CO_EMPLOYER
    WHERE FIRST_EMPLOYED_WORKERS_DATE IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM FIRST_EMPLOYED_WORKERS_DATE)
    ORDER BY year DESC
""")
total = 0
for row in cursor.fetchall():
    year = int(row[0]) if row[0] else 'NULL'
    count = row[1]
    total += count
    marker = ' ← 2015+' if year and year >= 2015 else ''
    print(f'  {year}: {count:,} employers{marker}')
print(f'\nTotal with FIRST_EMPLOYED_WORKERS_DATE: {total:,}')

# Count without date
cursor.execute("""
    SELECT COUNT(*) FROM SCH_CO_20.CO_EMPLOYER 
    WHERE FIRST_EMPLOYED_WORKERS_DATE IS NULL
""")
null_count = cursor.fetchone()[0]
print(f'Without FIRST_EMPLOYED_WORKERS_DATE: {null_count:,}')

# Count 2015+
cursor.execute("""
    SELECT COUNT(*) FROM SCH_CO_20.CO_EMPLOYER 
    WHERE FIRST_EMPLOYED_WORKERS_DATE >= TO_DATE('2015-01-01', 'YYYY-MM-DD')
""")
recent_count = cursor.fetchone()[0]
print(f'\nEmployers with FIRST_EMPLOYED_WORKERS_DATE >= 2015: {recent_count:,}')

conn.close()
