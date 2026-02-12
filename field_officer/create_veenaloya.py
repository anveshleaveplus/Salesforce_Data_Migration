"""
Get VEENALOYA details and create user
"""

import os
import csv
from dotenv import load_dotenv
import oracledb
from simple_salesforce import Salesforce

load_dotenv()

# Get from Oracle
conn = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = conn.cursor()

print("Getting VEENALOYA details from CO_USER...")

query = """
    SELECT 
        USERID,
        USER_NAME,
        IS_ACTIVE,
        JOB_TITLE,
        MOBILE_PHONE_NO
    FROM SCH_CO_20.CO_USER
    WHERE USERID = 'VEENALOYA'
"""

cursor.execute(query)
result = cursor.fetchone()

if result:
    print(f"✓ Found: {result[0]}")
    print(f"  Name: {result[1]}")
    print(f"  Active: {result[2]}")
    print(f"  Job: {result[3]}")
    print(f"  Mobile: {result[4]}")
    
    # Count assignments
    query2 = """
        SELECT COUNT(DISTINCT fvm.CUSTOMER_ID)
        FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT fov
        JOIN SCH_CO_20.CO_FIELD_VISIT_MEMBERS fvm 
            ON fov.FIELD_OFFICER_VISIT_ID = fvm.FIELD_OFFICER_VISIT_ID
        WHERE fov.ASSIGNED_TO = 'VEENALOYA'
    """
    cursor.execute(query2)
    workers = cursor.fetchone()[0]
    print(f"  Workers: {workers}")
    
    cursor.close()
    conn.close()
    
    # Create SF user
    print("\nCreating Salesforce user...")
    sf = Salesforce(
        username=os.getenv('SF_USERNAME'),
        password=os.getenv('SF_PASSWORD'),
        security_token=os.getenv('SF_SECURITY_TOKEN'),
        domain='test'
    )
    
    profiles = sf.query("SELECT Id FROM Profile WHERE Name = 'Standard User' LIMIT 1")
    profile_id = profiles['records'][0]['Id']
    
    name = result[1] if result[1] else 'Veena Loya'
    name_parts = name.split(' ', 1)
    first_name = name_parts[0]
    last_name = name_parts[1] if len(name_parts) > 1 else 'Officer'
    
    email = f"{name.lower().replace(' ', '.')}.sit@leaveplus.com.au"
    
    try:
        sf_result = sf.User.create({
            'Username': email,
            'Email': email,
            'FirstName': first_name,
            'LastName': last_name,
            'Alias': 'VEENALO',
            'CommunityNickname': 'VEENALOYA',
            'ProfileId': profile_id,
            'TimeZoneSidKey': 'Australia/Sydney',
            'LocaleSidKey': 'en_AU',
            'EmailEncodingKey': 'UTF-8',
            'LanguageLocaleKey': 'en_US',
            'MobilePhone': result[4] if result[4] else None,
            'IsActive': result[2] == 'Y'
        })
        
        print(f"✅ Created: {sf_result['id']}")
        
        # Add to CSV
        with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        
        data.append({
            'Officer_Code': 'VEENALOYA',
            'Salesforce_User_Id': sf_result['id'],
            'Email': email,
            'Status': 'Created',
            'Is_Active': result[2]
        })
        
        data.sort(key=lambda x: x['Officer_Code'])
        
        with open('field_officer_salesforce_mapping.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['Officer_Code', 'Salesforce_User_Id', 'Email', 'Status', 'Is_Active'])
            writer.writeheader()
            writer.writerows(data)
        
        print(f"✅ Added to mapping CSV")
        print(f"\n🎉 NOW HAVE ALL 47 FIELD OFFICERS!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
else:
    print("✗ VEENALOYA not found in CO_USER")
    cursor.close()
    conn.close()
