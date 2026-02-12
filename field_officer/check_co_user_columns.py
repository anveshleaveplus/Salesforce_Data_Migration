"""
List all columns in CO_USER table to see what's available
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

print("=" * 80)
print("CO_USER TABLE STRUCTURE")
print("=" * 80)

# Query table structure
query = """
    SELECT 
        COLUMN_NAME, 
        DATA_TYPE,
        DATA_LENGTH,
        NULLABLE
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SCH_CO_20' 
    AND TABLE_NAME = 'CO_USER'
    ORDER BY COLUMN_ID
"""

cursor.execute(query)
columns = cursor.fetchall()

print(f"\nFound {len(columns)} columns in CO_USER:\n")
print(f"{'Column Name':<35} {'Data Type':<20} {'Length':<10} {'Nullable'}")
print("-" * 80)

email_related = []
for col in columns:
    col_name, data_type, length, nullable = col
    print(f"{col_name:<35} {data_type:<20} {length:<10} {nullable}")
    
    # Look for email-related columns
    if 'EMAIL' in col_name or 'MAIL' in col_name or 'CONTACT' in col_name:
        email_related.append(col_name)

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

if email_related:
    print(f"✅ Found {len(email_related)} email-related columns:")
    for col in email_related:
        print(f"   - {col}")
    print("\nWe can use these columns for Field Officer emails!")
else:
    print("❌ No email-related columns found in CO_USER table")
    print("\nThis confirms:")
    print("   - Oracle doesn't store Field Officer email addresses")
    print("   - We must use generated emails (.sit2 format)")
    print("   - Current approach is correct")

print("=" * 80)
