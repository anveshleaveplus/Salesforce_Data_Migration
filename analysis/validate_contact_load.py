"""
Pre-load validation: Check for duplicates and datatype issues
Run BEFORE sit_contact_load.py to verify data quality
"""

import os
from dotenv import load_dotenv
import oracledb
import pandas as pd

load_dotenv()

conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

LIMIT_ROWS = 50000
ACTIVE_PERIOD = 202301

print("=" * 80)
print("CONTACT LOAD VALIDATION - DUPLICATES & DATATYPES")
print("=" * 80)

# Query same data as load script
query = f"""
SELECT * FROM (
    SELECT 
        w.CUSTOMER_ID as WORKER_ID,
        p.PERSON_ID,
        p.FIRST_NAME,
        p.SURNAME as LAST_NAME,
        p.DATE_OF_BIRTH,
        p.LANGUAGE_CODE,
        p.TITLE_CODE,
        p.GENDER_CODE,
        w.UNION_DELEGATE_CODE,
        c.EMAIL_ADDRESS,
        c.TELEPHONE1_NO,
        c.TELEPHONE2_NO,
        c.MOBILE_PHONE_NO,
        ep.EMPLOYER_ID,
        ROW_NUMBER() OVER (
            PARTITION BY w.CUSTOMER_ID 
            ORDER BY ep.EFFECTIVE_FROM_DATE DESC NULLS LAST
        ) as rn
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
    INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
    LEFT JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep 
        ON w.CUSTOMER_ID = ep.WORKER_ID 
        AND ep.EFFECTIVE_TO_DATE IS NULL
        AND ep.EMPLOYER_ID IN (
            SELECT DISTINCT e.CUSTOMER_ID
            FROM SCH_CO_20.CO_EMPLOYER e
            WHERE EXISTS (
                SELECT 1
                FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep2
                INNER JOIN SCH_CO_20.CO_SERVICE s 
                    ON s.WORKER = ep2.WORKER_ID
                WHERE ep2.EMPLOYER_ID = e.CUSTOMER_ID
                    AND s.PERIOD_END >= {ACTIVE_PERIOD}
            )
        )
) WHERE rn = 1 AND ROWNUM <= {LIMIT_ROWS}
"""

print(f"\nExtracting {LIMIT_ROWS:,} contact records...")
df = pd.read_sql(query, conn)
print(f"✓ Extracted {len(df):,} records\n")

# [1] DUPLICATE CHECKS
print("=" * 80)
print("[1] DUPLICATE CHECKS")
print("=" * 80)

print("\n1.1 External_Id__c (WORKER_ID) Duplicates:")
print("-" * 80)
worker_id_counts = df['WORKER_ID'].value_counts()
duplicates = worker_id_counts[worker_id_counts > 1]

if len(duplicates) > 0:
    print(f"❌ FOUND {len(duplicates)} duplicate WORKER_IDs:")
    for worker_id, count in duplicates.head(10).items():
        print(f"   WORKER_ID {worker_id}: {count} occurrences")
    if len(duplicates) > 10:
        print(f"   ... and {len(duplicates) - 10} more")
else:
    print(f"✓ NO duplicates - All {len(df):,} WORKER_IDs are unique")

print("\n1.2 NULL External_Id__c:")
print("-" * 80)
null_worker_ids = df['WORKER_ID'].isna().sum()
if null_worker_ids > 0:
    print(f"❌ FOUND {null_worker_ids:,} NULL WORKER_IDs")
else:
    print(f"✓ NO NULL WORKER_IDs")

print("\n1.3 Email Duplicates:")
print("-" * 80)
email_counts = df[df['EMAIL_ADDRESS'].notna()]['EMAIL_ADDRESS'].value_counts()
dup_emails = email_counts[email_counts > 1]
if len(dup_emails) > 0:
    print(f"⚠ Found {len(dup_emails):,} duplicate emails across {dup_emails.sum():,} contacts")
    print(f"   Top 5 duplicates:")
    for email, count in dup_emails.head(5).items():
        print(f"   {email}: {count} contacts")
else:
    print(f"✓ NO duplicate emails")

# [2] DATATYPE CHECKS
print("\n" + "=" * 80)
print("[2] DATATYPE CHECKS")
print("=" * 80)

print("\n2.1 WORKER_ID (must be integer/number):")
print("-" * 80)
try:
    df['WORKER_ID'].astype(int)
    print(f"✓ All {len(df):,} WORKER_IDs are valid integers")
except Exception as e:
    print(f"❌ ERROR: Cannot convert WORKER_ID to integer: {e}")
    non_numeric = df[pd.to_numeric(df['WORKER_ID'], errors='coerce').isna()]
    print(f"   Found {len(non_numeric)} non-numeric WORKER_IDs:")
    print(non_numeric['WORKER_ID'].head(10).tolist())

print("\n2.2 DATE_OF_BIRTH (must be valid date):")
print("-" * 80)
dob_count = df['DATE_OF_BIRTH'].notna().sum()
print(f"✓ {dob_count:,} records have DATE_OF_BIRTH")
print(f"  {len(df) - dob_count:,} records have NULL DATE_OF_BIRTH")

print("\n2.3 Email Format:")
print("-" * 80)
email_count = df['EMAIL_ADDRESS'].notna().sum()
emails_with_at = df[df['EMAIL_ADDRESS'].notna()]['EMAIL_ADDRESS'].str.contains('@', na=False).sum()
print(f"✓ {email_count:,} records have EMAIL_ADDRESS")
print(f"  {emails_with_at:,} emails contain '@' ({emails_with_at/email_count*100:.1f}%)")
if emails_with_at < email_count:
    invalid_emails = df[(df['EMAIL_ADDRESS'].notna()) & (~df['EMAIL_ADDRESS'].str.contains('@', na=False))]
    print(f"  ⚠ {len(invalid_emails)} emails without '@':")
    print(f"    {invalid_emails['EMAIL_ADDRESS'].head(5).tolist()}")

print("\n2.4 Phone Number Length:")
print("-" * 80)
for col in ['TELEPHONE1_NO', 'TELEPHONE2_NO', 'MOBILE_PHONE_NO']:
    if col in df.columns:
        non_null = df[col].notna().sum()
        if non_null > 0:
            lengths = df[df[col].notna()][col].astype(str).str.len()
            print(f"{col}:")
            print(f"  Count: {non_null:,}")
            print(f"  Length range: {lengths.min()}-{lengths.max()} chars")
            print(f"  Avg length: {lengths.mean():.1f} chars")

print("\n2.5 UnionDelegate__c Values:")
print("-" * 80)
union_values = df['UNION_DELEGATE_CODE'].value_counts()
print(f"UNION_DELEGATE_CODE distribution:")
print(f"  Total records: {len(df):,}")
print(f"  With union code: {df['UNION_DELEGATE_CODE'].notna().sum():,}")
print(f"  Top values:")
for val, count in union_values.head(10).items():
    will_be = "False" if str(val).strip() == '0' else "True"
    print(f"    {val}: {count:,} → UnionDelegate__c = {will_be}")

# [3] REFERENTIAL INTEGRITY
print("\n" + "=" * 80)
print("[3] REFERENTIAL INTEGRITY")
print("=" * 80)

print("\n3.1 PERSON_ID consistency:")
print("-" * 80)
person_id_count = df['PERSON_ID'].notna().sum()
if person_id_count == len(df):
    print(f"✓ All {len(df):,} records have PERSON_ID")
else:
    print(f"❌ {len(df) - person_id_count:,} records missing PERSON_ID")

print("\n3.2 EMPLOYER_ID (AccountId lookup):")
print("-" * 80)
with_employer = df['EMPLOYER_ID'].notna().sum()
without_employer = df['EMPLOYER_ID'].isna().sum()
print(f"  With EMPLOYER_ID: {with_employer:,} ({with_employer/len(df)*100:.1f}%)")
print(f"  Without EMPLOYER_ID: {without_employer:,} ({without_employer/len(df)*100:.1f}%)")
if without_employer > 0:
    print(f"  ⚠ {without_employer:,} contacts will have NULL AccountId")

# [4] SUMMARY
print("\n" + "=" * 80)
print("[4] VALIDATION SUMMARY")
print("=" * 80)

issues = []
if len(duplicates) > 0:
    issues.append(f"❌ {len(duplicates)} duplicate WORKER_IDs")
if null_worker_ids > 0:
    issues.append(f"❌ {null_worker_ids} NULL WORKER_IDs")
if emails_with_at < email_count:
    issues.append(f"⚠ {email_count - emails_with_at} invalid emails")
if without_employer > 0:
    issues.append(f"⚠ {without_employer:,} contacts without employer")

if len(issues) == 0:
    print("✓ ALL CHECKS PASSED - Safe to proceed with load")
else:
    print(f"Found {len(issues)} issue(s):")
    for issue in issues:
        print(f"  {issue}")
    print("\nRecommendation: Review issues before loading")

conn.close()

print("\n" + "=" * 80)
print("VALIDATION COMPLETE")
print("=" * 80)
