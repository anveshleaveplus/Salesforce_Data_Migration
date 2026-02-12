"""
Check ALPHA_CHECK values and document code sets (simplified)
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
print("ALPHA_CHECK AND DOCUMENT CODE SETS (SIMPLIFIED)")
print("=" * 80)

# ALPHA_CHECK top values
print("\n[1] ALPHA_CHECK Top 20 Values:")
print("-" * 80)

query = """
    SELECT ALPHA_CHECK, COUNT(*) as cnt
    FROM CO_CUSTOMER
    WHERE ALPHA_CHECK IS NOT NULL
    GROUP BY ALPHA_CHECK
    ORDER BY COUNT(*) DESC
    FETCH FIRST 20 ROWS ONLY
"""

cursor.execute(query)
results = cursor.fetchall()

total_query = "SELECT COUNT(*) FROM CO_CUSTOMER WHERE ALPHA_CHECK IS NOT NULL"
cursor.execute(total_query)
total_with_alpha = cursor.fetchone()[0]

null_query = "SELECT COUNT(*) FROM CO_CUSTOMER WHERE ALPHA_CHECK IS NULL"
cursor.execute(null_query)
total_null = cursor.fetchone()[0]

print(f"Top 20 ALPHA_CHECK values (from {total_with_alpha:,} non-NULL):")
print(f"{'Value':<15} {'Count':>12} {'%':>8}")
print("-" * 37)
for value, count in results:
    pct = (count / total_with_alpha * 100) if total_with_alpha > 0 else 0
    print(f"{value:<15} {count:>12,} {pct:>7.2f}%")
    
print(f"\nNULL values: {total_null:,}")
print(f"Non-NULL values: {total_with_alpha:,}")
print(f"Total: {total_null + total_with_alpha:,}")

# DocumentClassType code set
print("\n[2] Code Set 102: DocumentClassType:")
print("-" * 80)

query = """
    SELECT CODE, DESCRIPTION
    FROM CO_CODE
    WHERE CODE_SET_ID = 102
    ORDER BY CODE
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    for code, desc in results:
        print(f"  {code}: {desc}")
else:
    print("No results")

# DocumentIndexCode code set  
print("\n[3] Code Set 197: DocumentIndexCode:")
print("-" * 80)

query = """
    SELECT CODE, DESCRIPTION
    FROM CO_CODE
    WHERE CODE_SET_ID = 197
    ORDER BY CODE
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    for code, desc in results:
        print(f"  {code}: {desc}")
else:
    print("No results")

# WorkerDocumentUploadCode code set
print("\n[4] Code Set 318: WorkerDocumentUploadCode:")
print("-" * 80)

query = """
    SELECT CODE, DESCRIPTION
    FROM CO_CODE
    WHERE CODE_SET_ID = 318
    ORDER BY CODE
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    for code, desc in results:
        print(f"  {code}: {desc}")
else:
    print("No results")

# Check document tables
print("\n[5] Document Tables:")
print("-" * 80)

query = """
    SELECT TABLE_NAME
    FROM ALL_TABLES
    WHERE OWNER = 'SCH_CO_20'
    AND TABLE_NAME LIKE '%DOCUMENT%'
    ORDER BY TABLE_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} document tables:")
    for (table,) in results:
        print(f"  {table}")
else:
    print("No document tables")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("CONCLUSION:")
print("=" * 80)
print("ALPHA_CHECK contains 3-character codes (AAA, STE, etc.)")
print("These appear to be initials or classification codes, NOT ID verification status")
print("\nSF IDStatus__c (Verified/In Progress/Not Verified) has NO obvious Oracle mapping")
print("Recommendation: Leave IDStatus__c NULL or set default value in SF")
