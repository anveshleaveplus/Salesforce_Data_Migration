"""
Check if Field Officers have email addresses in Oracle CO_USER table
"""
import os
import oracledb
from dotenv import load_dotenv

env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print("=" * 100)
print("ORACLE FIELD OFFICER EMAIL CHECK")
print("=" * 100)

# Query Field Officers with email addresses
query = """
    SELECT 
        USERID,
        USER_NAME,
        EMAIL,
        IS_ACTIVE
    FROM SCH_CO_20.CO_USER
    WHERE USERID IN (
        SELECT DISTINCT ASSIGNED_TO 
        FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT
        WHERE ASSIGNED_TO IS NOT NULL
    )
    ORDER BY 
        CASE WHEN EMAIL IS NULL THEN 1 ELSE 0 END,  -- Emails first
        IS_ACTIVE DESC, 
        USERID
"""

cursor.execute(query)
rows = cursor.fetchall()

print(f"\nFound {len(rows)} Field Officers:\n")

# Count emails
has_email = sum(1 for row in rows if row[2] is not None)
no_email = sum(1 for row in rows if row[2] is None)

print(f"{'User ID':<20} {'Name':<35} {'Email':<45} {'Active'}")
print("-" * 105)

for row in rows:
    userid, user_name, email, active = row
    email_display = email if email else "(NULL)"
    active_flag = 'Y' if active == 'Y' else 'N'
    print(f"{userid:<20} {user_name or '':<35} {email_display:<45} {active_flag}")

cursor.close()
conn.close()

print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)
print(f"Total Field Officers: {len(rows)}")
print(f"✅ Have Email: {has_email} ({has_email/len(rows)*100:.1f}%)")
print(f"❌ No Email: {no_email} ({no_email/len(rows)*100:.1f}%)")

if has_email == len(rows):
    print("\n🎉 All Field Officers have email addresses in Oracle!")
    print("   We can use these emails for Salesforce users")
elif has_email == 0:
    print("\n⚠️  No Field Officers have email addresses in Oracle")
    print("   Must use generated emails (current .sit2 approach)")
else:
    print(f"\n⚠️  Only {has_email}/{len(rows)} Field Officers have emails")
    print("   Mixed approach needed: Use Oracle emails where available, generate for others")

print("=" * 100)
