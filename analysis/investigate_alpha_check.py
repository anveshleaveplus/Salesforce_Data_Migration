"""
Investigate ALPHA_CHECK column and document code sets
Check if these relate to ID verification status (Verified/In Progress/Not Verified)
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
print("ALPHA_CHECK AND DOCUMENT CODE SETS INVESTIGATION")
print("=" * 80)
print("\nSF IDStatus__c values: Verified, In Progress, Not Verified")

# Check ALPHA_CHECK column values
print("\n[1] CO_CUSTOMER.ALPHA_CHECK Value Distribution:")
print("-" * 80)

query = """
    SELECT ALPHA_CHECK, COUNT(*) as cnt
    FROM CO_CUSTOMER
    GROUP BY ALPHA_CHECK
    ORDER BY COUNT(*) DESC
"""

cursor.execute(query)
results = cursor.fetchall()

print(f"{'Value':<10} {'Count':>12} {'Percentage':>12}")
print("-" * 36)
total = sum(row[1] for row in results)
for value, count in results:
    display_val = str(value) if value is not None else 'NULL'
    pct = (count / total * 100) if total > 0 else 0
    print(f"{display_val:<10} {count:>12,} {pct:>11.2f}%")
print("-" * 36)
print(f"{'Total':<10} {total:>12,} {100:>11.2f}%")

# Sample records with ALPHA_CHECK values
print("\n[2] Sample CO_CUSTOMER Records with ALPHA_CHECK:")
print("-" * 80)

query = """
    SELECT 
        c.CUSTOMER_ID,
        c.ALPHA_CHECK,
        p.FIRST_NAME,
        p.LAST_NAME,
        c.IS_KEEP_INFO_PRIVATE
    FROM CO_CUSTOMER c
    JOIN CO_PERSON p ON c.PERSON_ID = p.PERSON_ID
    WHERE c.ALPHA_CHECK IS NOT NULL
    AND ROWNUM <= 20
    ORDER BY c.ALPHA_CHECK
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"{'CUSTOMER_ID':<12} {'ALPHA_CHECK':<12} {'Name':<30} {'PRIVATE':<8}")
    print("-" * 64)
    for cust_id, alpha_check, first, last, private in results:
        name = f"{first or ''} {last or ''}".strip()
        print(f"{cust_id:<12} {alpha_check:<12} {name:<30} {str(private):<8}")
else:
    print("No records with ALPHA_CHECK values found")

# Check DocumentClassType code set
print("\n[3] Code Set 102: DocumentClassType:")
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
    print(f"Found {len(results)} document class types:")
    for code, desc in results:
        print(f"  {code}: {desc}")
else:
    print("No DocumentClassType codes found")

# Check DocumentIndexCode code set
print("\n[4] Code Set 197: DocumentIndexCode:")
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
    print(f"Found {len(results)} document index codes:")
    for code, desc in results:
        print(f"  {code}: {desc}")
else:
    print("No DocumentIndexCode codes found")

# Check WorkerDocumentUploadCode code set
print("\n[5] Code Set 318: WorkerDocumentUploadCode:")
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
    print(f"Found {len(results)} worker document upload codes:")
    for code, desc in results:
        print(f"  {code}: {desc}")
else:
    print("No WorkerDocumentUploadCode codes found")

# Check if there are document-related tables
print("\n[6] Document-Related Tables:")
print("-" * 80)

query = """
    SELECT TABLE_NAME
    FROM ALL_TABLES
    WHERE OWNER = 'SCH_CO_20'
    AND (TABLE_NAME LIKE '%DOCUMENT%' OR TABLE_NAME LIKE '%DOC%')
    ORDER BY TABLE_NAME
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} document-related tables:")
    for (table_name,) in results:
        print(f"  {table_name}")
        
    # Check structure of main document table if exists
    if any('CO_DOCUMENT' in row[0] for row in results):
        print("\n  Checking CO_DOCUMENT table structure:")
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = 'SCH_CO_20'
            AND TABLE_NAME = 'CO_DOCUMENT'
            AND (COLUMN_NAME LIKE '%STATUS%' 
                 OR COLUMN_NAME LIKE '%TYPE%'
                 OR COLUMN_NAME LIKE '%CODE%'
                 OR COLUMN_NAME LIKE '%CUSTOMER%'
                 OR COLUMN_NAME LIKE '%PERSON%')
            ORDER BY COLUMN_NAME
        """
        cursor.execute(query)
        doc_cols = cursor.fetchall()
        for col, dtype, length in doc_cols:
            print(f"    {col} ({dtype}({length}))")
else:
    print("No document tables found")

# Check active contacts coverage with ALPHA_CHECK
print("\n[7] Active Contacts with ALPHA_CHECK:")
print("-" * 80)

query = """
    SELECT 
        COUNT(DISTINCT c.CUSTOMER_ID) as total_active,
        COUNT(DISTINCT CASE WHEN c.ALPHA_CHECK IS NOT NULL THEN c.CUSTOMER_ID END) as with_alpha_check,
        ROUND(COUNT(DISTINCT CASE WHEN c.ALPHA_CHECK IS NOT NULL THEN c.CUSTOMER_ID END) * 100.0 / 
              NULLIF(COUNT(DISTINCT c.CUSTOMER_ID), 0), 2) as coverage_pct
    FROM CO_CUSTOMER c
    JOIN CO_WORKER w ON c.CUSTOMER_ID = w.CUSTOMER_ID
    WHERE w.EMPLOYER_ID IN (
        SELECT DISTINCT e.EMPLOYER_ID
        FROM CO_EMPLOYER e
        JOIN CO_SERVICE_PERIOD sp ON e.EMPLOYER_ID = sp.EMPLOYER_ID
        WHERE sp.SERVICE_PERIOD >= 202301
    )
"""

cursor.execute(query)
result = cursor.fetchone()

if result:
    total_active, with_alpha, coverage = result
    print(f"Total active contacts: {total_active:,}")
    print(f"With ALPHA_CHECK: {with_alpha:,}")
    print(f"Coverage: {coverage:.2f}%")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("INVESTIGATION COMPLETE")
print("=" * 80)
print("\nConclusion:")
print("- ALPHA_CHECK: Likely an internal code, need to see values")
print("- Document code sets: Check if related to ID verification documents")
print("- May need to use a default value or leave IDStatus__c as NULL for now")
