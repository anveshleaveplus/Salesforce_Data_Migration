"""
Verify actual count from CO_FIELD_OFFICER_VISIT
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

print("Querying unique Field Officer codes from CO_FIELD_OFFICER_VISIT...")

query = """
    SELECT DISTINCT ASSIGNED_TO
    FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT
    WHERE ASSIGNED_TO IS NOT NULL
    ORDER BY ASSIGNED_TO
"""

cursor.execute(query)
results = cursor.fetchall()

officer_codes = [row[0] for row in results]

print(f"\nTotal unique Field Officer codes: {len(officer_codes)}")

print("\nAll codes:")
for i, code in enumerate(officer_codes, 1):
    print(f"  {i}. {code}")

# Check which exist in CO_USER
query2 = """
    SELECT USERID 
    FROM SCH_CO_20.CO_USER
    WHERE USERID IN ({})
""".format(','.join([f"'{code}'" for code in officer_codes]))

cursor.execute(query2)
in_user = [row[0] for row in cursor.fetchall()]

print(f"\n✓ Found in CO_USER: {len(in_user)}")
print(f"✗ NOT in CO_USER: {len(officer_codes) - len(in_user)}")

missing = set(officer_codes) - set(in_user)
if missing:
    print(f"\nMissing codes:")
    for code in sorted(missing):
        print(f"  - {code}")

cursor.close()
conn.close()
