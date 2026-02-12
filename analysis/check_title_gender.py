import oracledb
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

cursor = conn.cursor()

print("=" * 80)
print("CO_PERSON TITLE_CODE and GENDER_CODE Analysis")
print("=" * 80)

# Check title_code values
print("\n1. TITLE_CODE Distribution:")
print("-" * 80)
cursor.execute("""
    SELECT 
        p.TITLE_CODE,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
    FROM SCH_CO_20.CO_PERSON p
    INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
    INNER JOIN SCH_CO_20.CO_WORKER w ON c.CUSTOMER_ID = w.CUSTOMER_ID
    GROUP BY p.TITLE_CODE
    ORDER BY count DESC
""")

for row in cursor:
    title_code = row[0] if row[0] else 'NULL'
    count = row[1]
    pct = row[2]
    print(f"   {title_code:<20} {count:>10,} ({pct:>6}%)")

# Check gender_code values
print("\n2. GENDER_CODE Distribution:")
print("-" * 80)
cursor.execute("""
    SELECT 
        p.GENDER_CODE,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
    FROM SCH_CO_20.CO_PERSON p
    INNER JOIN SCH_CO_20.CO_CUSTOMER c ON p.PERSON_ID = c.CUSTOMER_ID
    INNER JOIN SCH_CO_20.CO_WORKER w ON c.CUSTOMER_ID = w.CUSTOMER_ID
    GROUP BY p.GENDER_CODE
    ORDER BY count DESC
""")

for row in cursor:
    gender_code = row[0] if row[0] else 'NULL'
    count = row[1]
    pct = row[2]
    print(f"   {gender_code:<20} {count:>10,} ({pct:>6}%)")

# Check if there's a code set for title
print("\n3. Title Code Set Check:")
print("-" * 80)
cursor.execute("""
    SELECT cs.CODE_SET_ID, cs.CODE_SET_NAME
    FROM SCH_CO_20.CO_CODE_SET cs
    WHERE UPPER(cs.CODE_SET_NAME) LIKE '%TITLE%'
""")

title_sets = cursor.fetchall()
if title_sets:
    for row in title_sets:
        print(f"   Found: Code Set {row[0]} - {row[1]}")
        # Get the codes
        cursor2 = conn.cursor()
        cursor2.execute("""
            SELECT VALUE, DESCRIPTION
            FROM SCH_CO_20.CO_CODE
            WHERE CODE_SET_ID = :code_set_id
            ORDER BY VALUE
        """, [row[0]])
        print(f"   Values:")
        for code_row in cursor2:
            print(f"      {code_row[0]} -> {code_row[1]}")
        cursor2.close()
else:
    print("   No code set found for TITLE")

# Check if there's a code set for gender
print("\n4. Gender Code Set Check:")
print("-" * 80)
cursor.execute("""
    SELECT cs.CODE_SET_ID, cs.CODE_SET_NAME
    FROM SCH_CO_20.CO_CODE_SET cs
    WHERE UPPER(cs.CODE_SET_NAME) LIKE '%GENDER%' 
       OR UPPER(cs.CODE_SET_NAME) LIKE '%SEX%'
""")

gender_sets = cursor.fetchall()
if gender_sets:
    for row in gender_sets:
        print(f"   Found: Code Set {row[0]} - {row[1]}")
        # Get the codes
        cursor2 = conn.cursor()
        cursor2.execute("""
            SELECT VALUE, DESCRIPTION
            FROM SCH_CO_20.CO_CODE
            WHERE CODE_SET_ID = :code_set_id
            ORDER BY VALUE
        """, [row[0]])
        print(f"   Values:")
        for code_row in cursor2:
            print(f"      {code_row[0]} -> {code_row[1]}")
        cursor2.close()
else:
    print("   No code set found for GENDER")

# Sample records
print("\n5. Sample Records:")
print("-" * 80)
cursor.execute("""
    SELECT 
        w.CUSTOMER_ID,
        p.TITLE_CODE,
        p.GENDER_CODE,
        p.FIRST_NAME,
        p.SURNAME
    FROM SCH_CO_20.CO_WORKER w
    INNER JOIN SCH_CO_20.CO_PERSON p ON w.PERSON_ID = p.PERSON_ID
    WHERE ROWNUM <= 10
""")

for row in cursor:
    print(f"   Worker {row[0]}: title='{row[1]}', gender='{row[2]}', name={row[3]} {row[4]}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
