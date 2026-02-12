"""
Check suitable Oracle field for DateEmploymentEndDate__c
Looking at CO_EMPLOYMENT_PERIOD.EFFECTIVE_TO_DATE
"""
import oracledb
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv('.env.sit')

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

print("Checking Employment End Date Fields")
print("="*80)

cursor = conn.cursor()

# Check CO_EMPLOYMENT_PERIOD date fields
print("\n1. CO_EMPLOYMENT_PERIOD date fields:")
cursor.execute("""
    SELECT 
        COUNT(*) as TOTAL_RECORDS,
        COUNT(EFFECTIVE_FROM_DATE) as HAS_START_DATE,
        COUNT(EFFECTIVE_TO_DATE) as HAS_END_DATE,
        COUNT(CASE WHEN EFFECTIVE_TO_DATE IS NULL THEN 1 END) as STILL_EMPLOYED,
        TO_CHAR(MIN(EFFECTIVE_FROM_DATE), 'YYYY-MM-DD') as MIN_START,
        TO_CHAR(MAX(EFFECTIVE_FROM_DATE), 'YYYY-MM-DD') as MAX_START,
        TO_CHAR(MIN(EFFECTIVE_TO_DATE), 'YYYY-MM-DD') as MIN_END,
        TO_CHAR(MAX(EFFECTIVE_TO_DATE), 'YYYY-MM-DD') as MAX_END
    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD
    WHERE EFFECTIVE_FROM_DATE > TO_DATE('1900-01-01', 'YYYY-MM-DD')
""")
result = cursor.fetchone()
print(f"   Total employment periods: {result[0]:,}")
print(f"   With start date (EFFECTIVE_FROM_DATE): {result[1]:,}")
print(f"   With end date (EFFECTIVE_TO_DATE): {result[2]:,}")
print(f"   Still employed (no end date): {result[3]:,}")
print(f"   Date range: {result[4]} to {result[5]} (start)")
print(f"   End date range: {result[6]} to {result[7]} (end)")

# Sample data
print("\n2. Sample employment period records with dates:")
cursor.execute("""
SELECT 
    ep.EMPLOYER_ID,
    e.TRADING_NAME,
    TO_CHAR(ep.EFFECTIVE_FROM_DATE, 'YYYY-MM-DD') as START_DATE,
    TO_CHAR(ep.EFFECTIVE_TO_DATE, 'YYYY-MM-DD') as END_DATE,
    CASE 
        WHEN ep.EFFECTIVE_TO_DATE IS NULL THEN 'Active'
        ELSE 'Ended'
    END as STATUS
FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
LEFT JOIN SCH_CO_20.CO_EMPLOYER e ON e.CUSTOMER_ID = ep.EMPLOYER_ID
WHERE ROWNUM <= 20
    AND ep.EFFECTIVE_FROM_DATE > TO_DATE('1900-01-01', 'YYYY-MM-DD')
ORDER BY ep.EFFECTIVE_FROM_DATE DESC
""")
print(f"{'EMPLOYER_ID':<15} {'TRADING_NAME':<40} {'START_DATE':<12} {'END_DATE':<12} {'STATUS':<10}")
print("-"*90)
for row in cursor.fetchmany(20):
    print(f"{str(row[0]):<15} {(row[1] or '')[:38]:<40} {row[2] or 'NULL':<12} {row[3] or 'NULL':<12} {row[4]:<10}")

# Check if there are other date fields in CO_WSR_SERVICE
print("\n3. Checking CO_WSR_SERVICE table columns:")
cursor.execute("""
    SELECT COLUMN_NAME 
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_WSR_SERVICE' 
    AND OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%DATE%' OR COLUMN_NAME LIKE '%END%')
    ORDER BY COLUMN_NAME
""")
print("   Date-related columns in CO_WSR_SERVICE:")
wsr_cols = []
for row in cursor:
    wsr_cols.append(row[0])
    print(f"      - {row[0]}")
if not wsr_cols:
    print("      (None found)")

# Check employer-level dates
print("\n4. Checking CO_EMPLOYER for employer end dates:")
cursor.execute("""
    SELECT COLUMN_NAME 
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_EMPLOYER' 
    AND OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%DATE%' OR COLUMN_NAME LIKE '%END%')
    ORDER BY COLUMN_NAME
""")
print("   Date-related columns in CO_EMPLOYER:")
for row in cursor:
    print(f"      - {row[0]}")

# Check CO_EMPLOYER_STATUS
print("\n5. Checking CO_EMPLOYER_STATUS for status end dates:")
cursor.execute("""
    SELECT COLUMN_NAME 
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = 'CO_EMPLOYER_STATUS' 
    AND OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%DATE%' OR COLUMN_NAME LIKE '%END%' OR COLUMN_NAME LIKE '%TO%')
    ORDER BY COLUMN_NAME
""")
print("   Date-related columns in CO_EMPLOYER_STATUS:")
for row in cursor:
    print(f"      - {row[0]}")

conn.close()

print("\n" + "="*80)
print("RECOMMENDATION:")
print("For DateEmploymentEndDate__c (employer-level end date):")
print()
print("OPTION 1 (Recommended - Latest Employment End):")
print("  Use MAX(EFFECTIVE_TO_DATE) from CO_EMPLOYMENT_PERIOD")
print("  This gives the most recent employment end date for that employer")
print()
print("OPTION 2 (WSR Service End):")
print("  Use EMPLOYMENT_END_DATE from CO_WSR_SERVICE")
print("  If available (check results above)")
print()
print("OPTION 3 (Employer Status Change Date):")
print("  Check CO_EMPLOYER_STATUS for status effective dates")
print("="*80)
