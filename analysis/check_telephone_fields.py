"""
Analyze TELEPHONE1_NO and TELEPHONE2_NO fields in CO_CUSTOMER
Check data coverage and patterns for 50K active contacts
"""
import oracledb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Oracle connection
conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

print("="*80)
print("TELEPHONE FIELDS ANALYSIS - ACTIVE CONTACTS (50K batch)")
print("="*80)

ACTIVE_PERIOD = 20231001

# Query active contacts with all phone fields
query = f"""
SELECT 
    w.CUSTOMER_ID,
    c.MOBILE_PHONE_NO,
    c.TELEPHONE1_NO,
    c.TELEPHONE2_NO
FROM (
    SELECT 
        w.CUSTOMER_ID,
        p.PERSON_ID,
        ROW_NUMBER() OVER (
            PARTITION BY w.CUSTOMER_ID 
            ORDER BY ep.EFFECTIVE_FROM_DATE DESC NULLS LAST
        ) as rn
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
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
    WHERE w.WORKER_STATUS_ID = 10
) w
INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
WHERE w.rn = 1
"""

cursor = conn.cursor()
cursor.execute(query)
results = cursor.fetchall()

total = len(results)
print(f"\nTotal Active Contacts: {total:,}\n")

# Count populated fields
mobile_count = sum(1 for r in results if r[1])
tel1_count = sum(1 for r in results if r[2])
tel2_count = sum(1 for r in results if r[3])

print("="*80)
print("FIELD COVERAGE")
print("="*80)
print(f"{'Field':<25} {'Populated':<15} {'NULL':<15} {'Coverage':<15}")
print("-"*80)
print(f"{'MOBILE_PHONE_NO':<25} {mobile_count:<15,} {total-mobile_count:<15,} {mobile_count/total*100:>6.2f}%")
print(f"{'TELEPHONE1_NO':<25} {tel1_count:<15,} {total-tel1_count:<15,} {tel1_count/total*100:>6.2f}%")
print(f"{'TELEPHONE2_NO':<25} {tel2_count:<15,} {total-tel2_count:<15,} {tel2_count/total*100:>6.2f}%")
print("="*80)

# Check overlap - contacts with both telephone fields
both_tel = sum(1 for r in results if r[2] and r[3])
only_tel1 = sum(1 for r in results if r[2] and not r[3])
only_tel2 = sum(1 for r in results if not r[2] and r[3])

print("\nTELEPHONE FIELD OVERLAP")
print("-"*80)
print(f"Both TELEPHONE1 & TELEPHONE2:  {both_tel:,} ({both_tel/total*100:.2f}%)")
print(f"Only TELEPHONE1:               {only_tel1:,} ({only_tel1/total*100:.2f}%)")
print(f"Only TELEPHONE2:               {only_tel2:,} ({only_tel2/total*100:.2f}%)")
print(f"Neither:                       {total-tel1_count-tel2_count+both_tel:,}")

# Sample data
print("\n" + "="*80)
print("SAMPLE RECORDS (First 15 with phone data)")
print("="*80)
print(f"{'CUSTOMER_ID':<12} {'MOBILE':<15} {'TELEPHONE1':<15} {'TELEPHONE2':<15}")
print("-"*80)

sample_count = 0
for r in results:
    if r[1] or r[2] or r[3]:  # Show records with at least one phone
        mobile = r[1] if r[1] else 'NULL'
        tel1 = r[2] if r[2] else 'NULL'
        tel2 = r[3] if r[3] else 'NULL'
        print(f"{r[0]:<12} {str(mobile)[:14]:<15} {str(tel1)[:14]:<15} {str(tel2)[:14]:<15}")
        sample_count += 1
        if sample_count >= 15:
            break

# Check if TELEPHONE2 has different data than TELEPHONE1
print("\n" + "="*80)
print("CONTACTS WITH BOTH TELEPHONE FIELDS (Sample 10)")
print("="*80)

both_query = f"""
SELECT 
    w.CUSTOMER_ID,
    c.TELEPHONE1_NO,
    c.TELEPHONE2_NO
FROM (
    SELECT 
        w.CUSTOMER_ID,
        p.PERSON_ID,
        ROW_NUMBER() OVER (
            PARTITION BY w.CUSTOMER_ID 
            ORDER BY ep.EFFECTIVE_FROM_DATE DESC NULLS LAST
        ) as rn
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
    LEFT JOIN SCH_CO_20.CO_EMPLOYMENT_PERIOD ep 
        ON w.CUSTOMER_ID = ep.WORKER_ID 
        AND ep.EFFECTIVE_TO_DATE IS NULL
    WHERE w.WORKER_STATUS_ID = 10
) w
INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
WHERE w.rn = 1 
    AND c.TELEPHONE1_NO IS NOT NULL 
    AND c.TELEPHONE2_NO IS NOT NULL
    AND ROWNUM <= 10
"""

cursor.execute(both_query)
both_samples = cursor.fetchall()

print(f"{'CUSTOMER_ID':<15} {'TELEPHONE1_NO':<25} {'TELEPHONE2_NO':<25}")
print("-"*80)
for r in both_samples:
    print(f"{r[0]:<15} {str(r[1]):<25} {str(r[2]):<25}")

print("\n" + "="*80)
print("RECOMMENDATION")
print("="*80)
print("Salesforce Phone Fields Available:")
print("  - Phone          (standard primary phone)")
print("  - MobilePhone    (standard mobile)")
print("  - HomePhone      (standard home)")
print("  - OtherPhone     (standard alternate)")
print()
print("Oracle Fields:")
print("  - MOBILE_PHONE_NO")
print("  - TELEPHONE1_NO")
print("  - TELEPHONE2_NO")
print()
print("Current mapping:")
print("  ✓ MobilePhone    ← MOBILE_PHONE_NO")
print("  ⚠ OtherPhone     ← TELEPHONE1_NO")
print("  ✗ Phone          ← (not mapped)")
print("  ✗ HomePhone      ← (not mapped)")
print("  ✗ TELEPHONE2_NO  ← (not used)")
print()
print("Option 1 (Primary/Secondary):")
print("  ✓ MobilePhone    ← MOBILE_PHONE_NO")
print("  ✓ Phone          ← TELEPHONE1_NO (primary landline)")
print("  ✓ OtherPhone     ← TELEPHONE2_NO (secondary)")
print()
print("Option 2 (Home/Other):")
print("  ✓ MobilePhone    ← MOBILE_PHONE_NO")
print("  ✓ HomePhone      ← TELEPHONE1_NO (home/landline)")
print("  ✓ OtherPhone     ← TELEPHONE2_NO (alternate)")
print()
print("Option 3 (All fields):")
print("  ✓ MobilePhone    ← MOBILE_PHONE_NO")
print("  ✓ Phone          ← TELEPHONE1_NO (primary)")
print("  ✓ HomePhone      ← TELEPHONE1_NO (duplicate if home)")
print("  ✓ OtherPhone     ← TELEPHONE2_NO (secondary)")
print()
print("❓ Need to know: What do TELEPHONE1_NO and TELEPHONE2_NO represent?")
print("   (home, work, primary, secondary, etc.)")
print("="*80)

cursor.close()
conn.close()
