"""
Test postal address (ShippingAddress) field
"""
import os
import oracledb
from dotenv import load_dotenv

load_dotenv('.env.sit')

connection = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

query = """
SELECT 
    e.CUSTOMER_ID,
    e.TRADING_NAME,
    TRIM(
        COALESCE(addr.STREET, '') || 
        CASE WHEN addr.STREET2 IS NOT NULL THEN ' ' || addr.STREET2 ELSE '' END || 
        CASE WHEN addr.SUBURB IS NOT NULL THEN ', ' || addr.SUBURB ELSE '' END || 
        CASE WHEN addr.STATE IS NOT NULL THEN ', ' || addr.STATE ELSE '' END || 
        CASE WHEN addr.POSTCODE IS NOT NULL THEN ', ' || addr.POSTCODE ELSE '' END || 
        CASE WHEN addr.COUNTRY_CODE IS NOT NULL THEN ', ' || addr.COUNTRY_CODE ELSE '' END
    ) as REGISTERED_ADDRESS,
    TRIM(
        COALESCE(postal_addr.STREET, '') || 
        CASE WHEN postal_addr.STREET2 IS NOT NULL THEN ' ' || postal_addr.STREET2 ELSE '' END || 
        CASE WHEN postal_addr.SUBURB IS NOT NULL THEN ', ' || postal_addr.SUBURB ELSE '' END || 
        CASE WHEN postal_addr.STATE IS NOT NULL THEN ', ' || postal_addr.STATE ELSE '' END || 
        CASE WHEN postal_addr.POSTCODE IS NOT NULL THEN ', ' || postal_addr.POSTCODE ELSE '' END || 
        CASE WHEN postal_addr.COUNTRY_CODE IS NOT NULL THEN ', ' || postal_addr.COUNTRY_CODE ELSE '' END
    ) as POSTAL_ADDRESS,
    CASE 
        WHEN c.POSTAL_ADDRESS_ID IS NOT NULL 
            AND c.ADDRESS_ID != c.POSTAL_ADDRESS_ID 
        THEN 1 
        ELSE 0 
    END as IS_POSTAL_DIFFERENT
FROM SCH_CO_20.CO_EMPLOYER e
LEFT JOIN SCH_CO_20.CO_CUSTOMER c ON c.CUSTOMER_ID = e.CUSTOMER_ID
LEFT JOIN SCH_CO_20.CO_ADDRESS addr ON addr.ADDRESS_ID = c.ADDRESS_ID
LEFT JOIN SCH_CO_20.CO_ADDRESS postal_addr ON postal_addr.ADDRESS_ID = c.POSTAL_ADDRESS_ID
WHERE e.CUSTOMER_ID IN (
    SELECT DISTINCT ep.EMPLOYER_ID
    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
    WHERE EXISTS (
        SELECT 1 FROM SCH_CO_20.CO_SERVICE s
        WHERE s.WORKER = ep.WORKER_ID
        AND s.PERIOD_END >= 202301
    )
)
AND ROWNUM <= 20
ORDER BY IS_POSTAL_DIFFERENT DESC, e.CUSTOMER_ID
"""

cursor = connection.cursor()
cursor.execute(query)

print("\n" + "="*100)
print("POSTAL ADDRESS (ShippingAddress) FIELD TEST")
print("="*100)

results = cursor.fetchall()

print("\n1. Records WITH DIFFERENT postal address (ShippingAddress will be populated):")
print("-"*100)
count = 0
for row in results:
    if row[4] == 1:  # IS_POSTAL_DIFFERENT
        count += 1
        print(f"\n{count}. {row[1]}")
        print(f"   RegisteredOfficeAddress__c: {row[2]}")
        print(f"   ShippingAddress:            {row[3]}")
        print(f"   IsPostalAddressDifferent:   TRUE")

print(f"\n\n2. Records with NO postal address (ShippingAddress will be empty/NULL):")
print("-"*100)
count = 0
for row in results:
    if row[4] == 0 and not row[3]:  # No postal address
        count += 1
        if count <= 5:  # Show first 5
            print(f"\n{count}. {row[1]}")
            print(f"   RegisteredOfficeAddress__c: {row[2]}")
            print(f"   ShippingAddress:            NULL (not populated)")
            print(f"   IsPostalAddressDifferent:   FALSE")

# Statistics
cursor.execute("""
SELECT 
    COUNT(*) as Total,
    SUM(CASE WHEN c.POSTAL_ADDRESS_ID IS NOT NULL THEN 1 ELSE 0 END) as Has_Postal,
    SUM(CASE WHEN c.POSTAL_ADDRESS_ID IS NOT NULL AND c.ADDRESS_ID != c.POSTAL_ADDRESS_ID THEN 1 ELSE 0 END) as Different_Postal
FROM SCH_CO_20.CO_EMPLOYER e
LEFT JOIN SCH_CO_20.CO_CUSTOMER c ON c.CUSTOMER_ID = e.CUSTOMER_ID
WHERE e.CUSTOMER_ID IN (
    SELECT DISTINCT ep.EMPLOYER_ID
    FROM SCH_CO_20.CO_EMPLOYMENT_PERIOD ep
    WHERE EXISTS (
        SELECT 1 FROM SCH_CO_20.CO_SERVICE s
        WHERE s.WORKER = ep.WORKER_ID
        AND s.PERIOD_END >= 202301
    )
)
""")

stats = cursor.fetchone()

print("\n" + "="*100)
print("SHIPPING ADDRESS POPULATION STATISTICS")
print("="*100)
print(f"Total Employers:                  {stats[0]:,}")
print(f"With Postal Address:              {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")
print(f"Different from Registered:        {stats[2]:,} ({stats[2]/stats[0]*100:.1f}%)")
print(f"\nShippingAddress will be populated: {stats[1]:,} records ({stats[1]/stats[0]*100:.1f}%)")
print(f"ShippingAddress will be NULL:      {stats[0]-stats[1]:,} records ({(stats[0]-stats[1])/stats[0]*100:.1f}%)")
print("="*100)

cursor.close()
connection.close()
