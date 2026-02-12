"""
Search Oracle for ID verification/status columns
Based on SF picklist: Verified, In Progress, Not Verified
"""

import os
from dotenv import load_dotenv
import oracledb

load_dotenv()

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print("=" * 80)
print("ORACLE VERIFICATION/ID STATUS COLUMN SEARCH")
print("=" * 80)

# Search for VERIFIED/VERIFICATION columns
print("\n[1] VERIFIED/VERIFICATION Columns:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%VERIF%' 
         OR COLUMN_NAME LIKE '%CONFIRM%'
         OR COLUMN_NAME LIKE '%VALID%'
         OR COLUMN_NAME LIKE '%APPROVAL%'
         OR COLUMN_NAME LIKE '%APPROVED%')
    AND TABLE_NAME IN ('CO_WORKER', 'CO_PERSON', 'CO_CUSTOMER', 
                       'CO_CONTACT', 'CO_CONTACT_PERSON')
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} verification-related columns:")
    for table, column, dtype, length in results:
        print(f"  {table}.{column} ({dtype}({length}))")
else:
    print("No verification columns found in worker/person/customer tables")

# Search for DOCUMENT columns
print("\n[2] DOCUMENT-Related Columns:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%DOCUMENT%' 
         OR COLUMN_NAME LIKE '%DOC_%'
         OR COLUMN_NAME LIKE '%LICENCE%'
         OR COLUMN_NAME LIKE '%LICENSE%'
         OR COLUMN_NAME LIKE '%PASSPORT%'
         OR COLUMN_NAME LIKE '%DRIVER%')
    AND TABLE_NAME IN ('CO_WORKER', 'CO_PERSON', 'CO_CUSTOMER', 
                       'CO_CONTACT', 'CO_CONTACT_PERSON')
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} document-related columns:")
    for table, column, dtype, length in results:
        print(f"  {table}.{column} ({dtype}({length}))")
else:
    print("No document columns found")

# Search for CHECK/CHECKED columns (ID checks)
print("\n[3] CHECK/SCREENING Columns:")
print("-" * 80)

query = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND (COLUMN_NAME LIKE '%CHECK%' 
         OR COLUMN_NAME LIKE '%SCREEN%'
         OR COLUMN_NAME LIKE '%CLEARANCE%')
    AND TABLE_NAME IN ('CO_WORKER', 'CO_PERSON', 'CO_CUSTOMER', 
                       'CO_CONTACT', 'CO_CONTACT_PERSON')
    ORDER BY TABLE_NAME, COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} check/screening columns:")
    for table, column, dtype, length in results:
        print(f"  {table}.{column} ({dtype}({length}))")
else:
    print("No check/screening columns found")

# Sample CO_PERSON records to see all status/flag columns
print("\n[4] All CO_PERSON Status/Flag/Code Columns:")
print("-" * 80)

query = """
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME = 'CO_PERSON'
    AND (COLUMN_NAME LIKE '%STATUS%' 
         OR COLUMN_NAME LIKE '%FLAG%'
         OR COLUMN_NAME LIKE '%CODE%'
         OR COLUMN_NAME LIKE '%TYPE%'
         OR COLUMN_NAME LIKE '%IND%')
    ORDER BY COLUMN_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} status/flag/code columns in CO_PERSON:")
    for column, dtype, length in results:
        print(f"  {column} ({dtype}({length}))")

# Check code sets for verification/ID status
print("\n[5] Code Sets with VERIF/ID/STATUS keywords:")
print("-" * 80)

query = """
    SELECT CODE_SET_ID, CODE_SET_NAME
    FROM CO_CODE_SET
    WHERE UPPER(CODE_SET_NAME) LIKE '%VERIF%'
       OR UPPER(CODE_SET_NAME) LIKE '%STATUS%'
       OR UPPER(CODE_SET_NAME) LIKE '%CHECK%'
       OR UPPER(CODE_SET_NAME) LIKE '%VALID%'
       OR UPPER(CODE_SET_NAME) LIKE '%DOCUMENT%'
    ORDER BY CODE_SET_ID
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} relevant code sets:")
    for code_set_id, name in results:
        print(f"  Code Set {code_set_id}: {name}")
else:
    print("No verification/status code sets found")

# Sample data from contacts with various status indicators
print("\n[6] Sample CO_CUSTOMER Status Indicators:")
print("-" * 80)

query = """
    SELECT 
        CUSTOMER_ID,
        IS_KEEP_INFO_PRIVATE,
        IS_NO_EMPLOYER_CONTACT,
        IS_KEEP_INFO_PRIVATE_UNION
    FROM CO_CUSTOMER
    WHERE ROWNUM <= 10
"""

cursor.execute(query)
results = cursor.fetchall()

print(f"Sample privacy/contact flags:")
print(f"{'CUSTOMER_ID':<12} {'PRIVATE':<8} {'NO_CONTACT':<12} {'UNION_PRIVATE':<14}")
print("-" * 50)
for row in results:
    cust_id, private, no_contact, union_priv = row
    print(f"{cust_id:<12} {str(private):<8} {str(no_contact):<12} {str(union_priv):<14}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("SEARCH COMPLETE")
print("=" * 80)
print("\nSF IDStatus__c picklist values: Verified, In Progress, Not Verified")
print("Oracle likely uses a status/flag column or code set for ID verification tracking")
