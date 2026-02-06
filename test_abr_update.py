"""
Quick test - Update 10 accounts with ABR data
"""

import os
from dotenv import load_dotenv
import pandas as pd
from simple_salesforce import Salesforce
import pyodbc

load_dotenv('.env.sit')

print("Testing ABR enrichment on 10 accounts...")

# Connect to SF
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

# Get 10 accounts
result = sf.query("SELECT Id, ABN__c FROM Account WHERE External_Id__c != null AND ABN__c != null LIMIT 10")
accounts = [r for r in result['records']]

print(f"Found {len(accounts)} accounts")

# Get ABR data for these ABNs
sql_conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER=cosql-test.coinvest.com.au;'
    f'DATABASE=AvatarWarehouse;'
    f'Trusted_Connection=yes;'
)

for acc in accounts[:3]:
    abn = acc['ABN__c'].replace(' ', '')
    print(f"\nAccount {acc['Id']}: ABN={abn}")
    
    cursor = sql_conn.cursor()
    cursor.execute(f"""
        SELECT [ABN Status], [ABN Registration - Date of Effect], [Main - Industry Class Code]
        FROM [datascience].[abr_cleaned]
        WHERE [Australian Business Number] = {abn}
    """)
    result = cursor.fetchone()
    
    if result:
        status, reg_date, code = result
        status_mapped = 'Registered' if status == 'Active' else status
        print(f"  Found in ABR: Status={status_mapped}, Date={reg_date}, Code={code}")
        
        # Try to update
        try:
            sf.Account.update(acc['Id'], {
                'AccountStatus__c': status_mapped,
                'ABNRegistrationDate__c': str(reg_date) if reg_date else None,
                'OSCACode__c': str(int(code)) if code else None
            })
            print(f"  ✅ Updated successfully")
        except Exception as e:
            print(f"  ❌ Update failed: {e}")
    else:
        print(f"  Not found in ABR")

sql_conn.close()
print("\nTest complete!")
