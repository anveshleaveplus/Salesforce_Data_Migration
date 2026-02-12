"""
Check original Field Officer emails from Oracle to see what they should be
"""
import os
import oracledb
from dotenv import load_dotenv

env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    dsn=os.getenv('ORACLE_DSN')
)

cursor = conn.cursor()

print("=" * 80)
print("ORACLE FIELD OFFICER EMAIL ADDRESSES")
print("=" * 80)

# Query Field Officers from Oracle
query = """
    SELECT 
        USER_CODE,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        IS_ACTIVE
    FROM SCH_CO_20.CO_USER
    WHERE USER_CODE IN (
        SELECT DISTINCT ASSIGNED_TO 
        FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT
        WHERE ASSIGNED_TO IS NOT NULL
    )
    ORDER BY IS_ACTIVE DESC, USER_CODE
"""

cursor.execute(query)
rows = cursor.fetchall()

print(f"\nFound {len(rows)} Field Officers in Oracle:\n")
print(f"{'Code':<20} {'Name':<35} {'Oracle Email':<50} {'Active'}")
print("-" * 110)

for row in rows:
    code, first, last, email, active = row
    name = f"{first or ''} {last or ''}".strip()
    email_display = email or "(NULL)"
    active_flag = 'Y' if active == 'Y' else 'N'
    print(f"{code:<20} {name:<35} {email_display:<50} {active_flag}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("OBSERVATION")
print("=" * 80)
print("If Oracle emails are:")
print("  - Production emails (e.g., michael.docherty@leaveplus.com.au)")
print("    → We SHOULD use .sit suffix to avoid conflicts")
print("  - Already NULL/empty")
print("    → We can use any email format")
print("  - Test emails")
print("    → Check if they're different from production")
print("=" * 80)
