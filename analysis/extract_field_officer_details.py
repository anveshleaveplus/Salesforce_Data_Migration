"""
Extract 47 Field Officer Details from Oracle
Check if we can get names, emails from Oracle user/employee tables
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
print("FIELD OFFICER DETAILS FROM ORACLE")
print("=" * 80)

# [1] Get 47 unique field officer codes
print("\n[1] 47 Field Officer Codes from CO_FIELD_OFFICER_VISIT:")
print("-" * 80)

query = """
    SELECT DISTINCT ASSIGNED_TO
    FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT
    WHERE ASSIGNED_TO IS NOT NULL
    ORDER BY ASSIGNED_TO
"""

cursor.execute(query)
results = cursor.fetchall()

officer_codes = [row[0] for row in results]
print(f"Found {len(officer_codes)} unique field officers:")
for code in officer_codes:
    print(f"  {code}")

# [2] Search for USER tables
print("\n[2] Searching for USER tables:")
print("-" * 80)

query = """
    SELECT table_name
    FROM all_tables
    WHERE owner = 'SCH_CO_20'
    AND (
        LOWER(table_name) LIKE '%user%'
        OR LOWER(table_name) LIKE '%employee%'
        OR LOWER(table_name) LIKE '%staff%'
        OR LOWER(table_name) LIKE '%person%'
    )
    ORDER BY table_name
"""

cursor.execute(query)
results = cursor.fetchall()

user_tables = [row[0] for row in results]
if user_tables:
    print(f"Found {len(user_tables)} user/employee tables:")
    for table in user_tables:
        print(f"  {table}")
else:
    print("No user/employee tables found")

# [3] Check CO_PERSON for field officer details
if 'CO_PERSON' in user_tables or True:
    print("\n[3] Check CO_PERSON for field officers:")
    print("-" * 80)
    
    # Try to find field officers in CO_PERSON
    query = """
        SELECT PERSON_ID, FIRST_NAME, SURNAME
        FROM SCH_CO_20.CO_PERSON
        WHERE ROWNUM <= 5
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        print(f"Sample CO_PERSON records:")
        for row in results:
            print(f"  {row[0]}: {row[1]} {row[2]}")
    except Exception as e:
        print(f"Error: {e}")

# [4] Check CO_CUSTOMER for user records
print("\n[4] Check CO_CUSTOMER for possible user/officer records:")
print("-" * 80)

query = """
    SELECT CUSTOMER_ID, EMAIL_ADDRESS
    FROM SCH_CO_20.CO_CUSTOMER
    WHERE ROWNUM <= 5
"""

try:
    cursor.execute(query)
    results = cursor.fetchall()
    print(f"Sample CO_CUSTOMER records:")
    for row in results:
        print(f"  {row[0]}: {row[1]}")
except Exception as e:
    print(f"Error: {e}")

# [5] Search for columns that might have field officer details
print("\n[5] Searching for USERNAME/LOGIN columns:")
print("-" * 80)

query = """
    SELECT table_name, column_name, data_type
    FROM all_tab_columns
    WHERE owner = 'SCH_CO_20'
    AND (
        LOWER(column_name) LIKE '%username%'
        OR LOWER(column_name) LIKE '%login%'
        OR LOWER(column_name) LIKE '%user_id%'
        OR LOWER(column_name) = 'assigned_to'
    )
    ORDER BY table_name, column_name
"""

cursor.execute(query)
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} username/login columns:")
    for table, column, dtype in results[:20]:
        print(f"  {table}.{column} ({dtype})")
else:
    print("No username/login columns found")

# [6] Try to find field officer details by ASSIGNED_TO pattern
print("\n[6] Search for field officer details by matching ASSIGNED_TO patterns:")
print("-" * 80)

# Try to find if ASSIGNED_TO codes match any person names
sample_officers = officer_codes[:5] if len(officer_codes) >= 5 else officer_codes

for officer_code in sample_officers:
    # Extract likely name parts from code (e.g., MICHAELD -> MICHAEL D)
    print(f"\n  Searching for: {officer_code}")
    
    # Try CO_PERSON with name matching
    query = f"""
        SELECT FIRST_NAME, SURNAME, PERSON_ID
        FROM SCH_CO_20.CO_PERSON
        WHERE UPPER(FIRST_NAME) LIKE '%{officer_code[:6]}%'
        OR UPPER(SURNAME) LIKE '%{officer_code[:6]}%'
        AND ROWNUM <= 3
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        if results:
            print(f"    Possible matches in CO_PERSON:")
            for fname, sname, pid in results:
                print(f"      {fname} {sname} (ID: {pid})")
        else:
            print(f"    No matches in CO_PERSON")
    except Exception as e:
        print(f"    Error: {e}")

print("\n" + "=" * 80)
print("RECOMMENDATION")
print("=" * 80)
print("\nIf Oracle doesn't have officer details:")
print("  1. Ask Field Operations Manager for the 47 officer details")
print("  2. Or ask Business team who knows these officers")
print("  3. They provide: Code → Full Name, Email")
print("\nIf Oracle has officer details:")
print("  We can extract from the tables found above")

cursor.close()
conn.close()
