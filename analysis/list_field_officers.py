"""
Simple check: Can we extract 47 field officer details from Oracle?
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
print("FIELD OFFICER LIST FROM ORACLE")
print("=" * 80)

# Get all unique field officers
query = """
    SELECT 
        ASSIGNED_TO,
        COUNT(DISTINCT fvm.CUSTOMER_ID) as worker_count
    FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT fov
    JOIN SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm 
        ON fov.FIELD_OFFICER_VISIT_ID = fvm.FIELD_OFFICER_VISIT_ID
    WHERE ASSIGNED_TO IS NOT NULL
    GROUP BY ASSIGNED_TO
    ORDER BY COUNT(DISTINCT fvm.CUSTOMER_ID) DESC
"""

cursor.execute(query)
results = cursor.fetchall()

print(f"\nFound {len(results)} field officers:\n")
print(f"{'#':<5} {'Officer Code':<20} {'Assignments':>15}")
print("-" * 42)

for i, (officer, count) in enumerate(results, 1):
    print(f"{i:<5} {officer:<20} {count:>15,}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"\nTotal field officers: {len(results)}")
print(f"Total assignments: {sum(count for _, count in results):,}")
print("\nOracle has the officer CODES but NOT their full details (names, emails)")
print("\nYou need to get details from:")
print("  - Field Operations Manager")
print("  - HR Department")  
print("  - Business team who knows these officers")
print("\nProvide this list to them and ask for:")
print("  Code → Full Name, Email Address")

cursor.close()
conn.close()
