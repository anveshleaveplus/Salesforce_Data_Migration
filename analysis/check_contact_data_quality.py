"""
Pre-load validation: Check for duplicates and data types in Contact data
"""
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import oracledb
import pandas as pd

# Load environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

LIMIT_ROWS = 50000
ACTIVE_PERIOD = 202301

print("=" * 80)
print("Contact Data Quality Check - Pre-Load Validation")
print("=" * 80)

# Connect to Oracle
conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

print("\n[1/5] Extracting Contact data from Oracle...")
cursor = conn.cursor()

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
        c.EMAIL_ADDRESS,
        c.TELEPHONE1_NO as OTHER_PHONE,
        c.MOBILE_PHONE_NO,
        c.ADDRESS_ID,
        a1.STREET as OTHER_STREET,
        a1.STREET2 as OTHER_STREET2,
        a1.SUBURB as OTHER_CITY,
        a1.STATE as OTHER_STATE,
        a1.POSTCODE as OTHER_POSTALCODE,
        a1.COUNTRY_CODE as OTHER_COUNTRY,
        c.POSTAL_ADDRESS_ID,
        a2.STREET as MAILING_STREET,
        a2.STREET2 as MAILING_STREET2,
        a2.SUBURB as MAILING_CITY,
        a2.STATE as MAILING_STATE,
        a2.POSTCODE as MAILING_POSTALCODE,
        a2.COUNTRY_CODE as MAILING_COUNTRY,
        ep.EMPLOYER_ID,
        ROW_NUMBER() OVER (
            PARTITION BY w.CUSTOMER_ID 
            ORDER BY ep.EFFECTIVE_FROM_DATE DESC NULLS LAST
        ) as rn
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
    INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
    LEFT JOIN SCH_CO_20.CO_ADDRESS a1 ON c.ADDRESS_ID = a1.ADDRESS_ID
    LEFT JOIN SCH_CO_20.CO_ADDRESS a2 ON c.POSTAL_ADDRESS_ID = a2.ADDRESS_ID
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
    WHERE c.CUSTOMER_ID != 23000
) WHERE rn = 1 AND ROWNUM <= {LIMIT_ROWS}
"""

cursor.execute(query)
columns = [col[0] for col in cursor.description]
rows = cursor.fetchall()
df = pd.DataFrame(rows, columns=columns)

print(f"   Extracted {len(df):,} records")

# Load code mappings
print("\n[2/5] Loading code set mappings...")
cursor.execute("SELECT value, description FROM SCH_CO_20.CO_CODE WHERE code_set_id = 357 ORDER BY value")
language_mapping = {str(row[0]): row[1] for row in cursor}
print(f"   Language codes: {len(language_mapping)}")

cursor.execute("SELECT value, description FROM SCH_CO_20.CO_CODE WHERE code_set_id = 24 ORDER BY value")
title_mapping = {str(row[0]): row[1] for row in cursor}
print(f"   Title codes: {len(title_mapping)}")

cursor.execute("SELECT value, description FROM SCH_CO_20.CO_CODE WHERE code_set_id = 11 ORDER BY value")
gender_mapping = {str(row[0]): row[1] for row in cursor}
print(f"   Gender codes: {len(gender_mapping)}")

cursor.close()
conn.close()

# Check for duplicates
print("\n[3/5] Checking for duplicates...")
print("-" * 80)

# Check WORKER_ID (External_Id__c)
worker_id_counts = df['WORKER_ID'].value_counts()
duplicates = worker_id_counts[worker_id_counts > 1]

if len(duplicates) > 0:
    print(f"   [WARNING] Found {len(duplicates)} duplicate WORKER_IDs!")
    print(f"   Top duplicates:")
    for worker_id, count in duplicates.head(10).items():
        print(f"      WORKER_ID {worker_id}: {count} occurrences")
else:
    print(f"   [OK] No duplicate WORKER_IDs found - all {len(df):,} records are unique")

# Check for NULL WORKER_IDs
null_worker_ids = df['WORKER_ID'].isna().sum()
if null_worker_ids > 0:
    print(f"   [ERROR] Found {null_worker_ids} NULL WORKER_IDs!")
else:
    print(f"   [OK] No NULL WORKER_IDs")

# Validate data types and values
print("\n[4/5] Validating data types and values...")
print("-" * 80)

# Check WORKER_ID type
print("\n   External_Id__c (WORKER_ID):")
print(f"      Type: {df['WORKER_ID'].dtype}")
print(f"      Sample: {df['WORKER_ID'].head(3).tolist()}")
print(f"      Nulls: {df['WORKER_ID'].isna().sum():,} ({df['WORKER_ID'].isna().sum()/len(df)*100:.2f}%)")

# Check names
print("\n   FirstName & LastName:")
print(f"      FirstName nulls: {df['FIRST_NAME'].isna().sum():,} ({df['FIRST_NAME'].isna().sum()/len(df)*100:.2f}%)")
print(f"      LastName nulls: {df['LAST_NAME'].isna().sum():,} ({df['LAST_NAME'].isna().sum()/len(df)*100:.2f}%)")
print(f"      Sample: {df['FIRST_NAME'].iloc[0]} {df['LAST_NAME'].iloc[0]}")

# Check dates
print("\n   Birthdate:")
print(f"      Type: {df['DATE_OF_BIRTH'].dtype}")
print(f"      Nulls: {df['DATE_OF_BIRTH'].isna().sum():,} ({df['DATE_OF_BIRTH'].isna().sum()/len(df)*100:.2f}%)")
if df['DATE_OF_BIRTH'].notna().any():
    sample_date = df[df['DATE_OF_BIRTH'].notna()]['DATE_OF_BIRTH'].iloc[0]
    print(f"      Sample: {sample_date}")

# Check email
print("\n   Email:")
print(f"      Populated: {df['EMAIL_ADDRESS'].notna().sum():,} ({df['EMAIL_ADDRESS'].notna().sum()/len(df)*100:.2f}%)")
print(f"      Empty strings: {(df['EMAIL_ADDRESS'] == '').sum():,}")
if df['EMAIL_ADDRESS'].notna().any():
    sample_email = df[df['EMAIL_ADDRESS'].notna()]['EMAIL_ADDRESS'].iloc[0]
    print(f"      Sample: {sample_email}")

# Check phones
print("\n   Phone fields:")
print(f"      OtherPhone populated: {df['OTHER_PHONE'].notna().sum():,} ({df['OTHER_PHONE'].notna().sum()/len(df)*100:.2f}%)")
print(f"      MobilePhone populated: {df['MOBILE_PHONE_NO'].notna().sum():,} ({df['MOBILE_PHONE_NO'].notna().sum()/len(df)*100:.2f}%)")

# Check language codes
print("\n   LanguagePreference__c:")
print(f"      Populated: {df['LANGUAGE_CODE'].notna().sum():,} ({df['LANGUAGE_CODE'].notna().sum()/len(df)*100:.2f}%)")
lang_dist = df['LANGUAGE_CODE'].value_counts().head(5)
for code, count in lang_dist.items():
    mapped = language_mapping.get(str(int(code)), 'Unknown')
    print(f"      {code} ({mapped}): {count:,} ({count/len(df)*100:.2f}%)")

# Check title codes
print("\n   Title:")
print(f"      Populated: {df['TITLE_CODE'].notna().sum():,} ({df['TITLE_CODE'].notna().sum()/len(df)*100:.2f}%)")
title_dist = df['TITLE_CODE'].value_counts().head(5)
for code, count in title_dist.items():
    mapped = title_mapping.get(str(code), 'Unknown')
    print(f"      {code} ({mapped}): {count:,} ({count/len(df)*100:.2f}%)")

# Check gender codes
print("\n   TitleType (Gender):")
print(f"      Populated: {df['GENDER_CODE'].notna().sum():,} ({df['GENDER_CODE'].notna().sum()/len(df)*100:.2f}%)")
gender_dist = df['GENDER_CODE'].value_counts()
for code, count in gender_dist.items():
    mapped = gender_mapping.get(str(code), 'Unknown')
    print(f"      {code} ({mapped}): {count:,} ({count/len(df)*100:.2f}%)")

# Check addresses
print("\n   OtherAddress (ADDRESS_ID):")
print(f"      Has ADDRESS_ID: {df['ADDRESS_ID'].notna().sum():,} ({df['ADDRESS_ID'].notna().sum()/len(df)*100:.2f}%)")
print(f"      OtherStreet populated: {df['OTHER_STREET'].notna().sum():,} ({df['OTHER_STREET'].notna().sum()/len(df)*100:.2f}%)")
print(f"      OtherCity populated: {df['OTHER_CITY'].notna().sum():,} ({df['OTHER_CITY'].notna().sum()/len(df)*100:.2f}%)")

print("\n   MailingAddress (POSTAL_ADDRESS_ID):")
print(f"      Has POSTAL_ADDRESS_ID: {df['POSTAL_ADDRESS_ID'].notna().sum():,} ({df['POSTAL_ADDRESS_ID'].notna().sum()/len(df)*100:.2f}%)")
print(f"      MailingStreet populated: {df['MAILING_STREET'].notna().sum():,} ({df['MAILING_STREET'].notna().sum()/len(df)*100:.2f}%)")
print(f"      MailingCity populated: {df['MAILING_CITY'].notna().sum():,} ({df['MAILING_CITY'].notna().sum()/len(df)*100:.2f}%)")

# Check EMPLOYER_ID
print("\n   AccountId (EMPLOYER_ID lookup):")
print(f"      Populated: {df['EMPLOYER_ID'].notna().sum():,} ({df['EMPLOYER_ID'].notna().sum()/len(df)*100:.2f}%)")
print(f"      Unique employers: {df['EMPLOYER_ID'].nunique():,}")

# Data quality summary
print("\n[5/5] Data Quality Summary")
print("-" * 80)

issues = []

if len(duplicates) > 0:
    issues.append(f"❌ {len(duplicates)} duplicate WORKER_IDs found")
else:
    print("✓ No duplicate External IDs")

if null_worker_ids > 0:
    issues.append(f"❌ {null_worker_ids} NULL WORKER_IDs")
else:
    print("✓ All records have External ID")

if df['FIRST_NAME'].isna().sum() > 0 or df['LAST_NAME'].isna().sum() > 0:
    issues.append(f"⚠️  Some records missing FirstName or LastName")
else:
    print("✓ All records have FirstName and LastName")

# Check for unmapped codes
unmapped_lang = df[df['LANGUAGE_CODE'].notna() & ~df['LANGUAGE_CODE'].apply(lambda x: str(int(x)) in language_mapping)]
if len(unmapped_lang) > 0:
    issues.append(f"⚠️  {len(unmapped_lang)} records with unmapped LANGUAGE_CODE")
else:
    print("✓ All language codes can be mapped")

unmapped_title = df[df['TITLE_CODE'].notna() & ~df['TITLE_CODE'].apply(lambda x: str(x) in title_mapping)]
if len(unmapped_title) > 0:
    issues.append(f"⚠️  {len(unmapped_title)} records with unmapped TITLE_CODE")
else:
    print("✓ All title codes can be mapped")

unmapped_gender = df[df['GENDER_CODE'].notna() & ~df['GENDER_CODE'].apply(lambda x: str(x) in gender_mapping)]
if len(unmapped_gender) > 0:
    issues.append(f"⚠️  {len(unmapped_gender)} records with unmapped GENDER_CODE")
else:
    print("✓ All gender codes can be mapped")

if len(issues) > 0:
    print("\nIssues found:")
    for issue in issues:
        print(f"   {issue}")
    print("\nRecommendation: Review and fix issues before loading")
else:
    print("\n✓ All validation checks passed - data is ready to load")

print("\n" + "=" * 80)
