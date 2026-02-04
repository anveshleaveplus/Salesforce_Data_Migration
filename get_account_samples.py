from dotenv import load_dotenv
import os
from simple_salesforce import Salesforce

env_file = '.env.sit'
load_dotenv(env_file)

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

# Query sample accounts
result = sf.query("""
    SELECT External_Id__c, Name, ABN__c, ACN__c, 
           RegisteredEntityName__c, TradingAs__c, 
           DateEmploymentCommenced__c, Registration_Number__c
    FROM Account 
    WHERE External_Id__c != NULL 
    ORDER BY CreatedDate DESC 
    LIMIT 10
""")

# Write to txt file
output_file = 'test_output/sit_account_samples.txt'
with open(output_file, 'w', encoding='utf-8') as f:
    f.write("="*80 + "\n")
    f.write("SIT ACCOUNT LOAD - SAMPLE RECORDS\n")
    f.write("="*80 + "\n")
    f.write(f"Date: February 3, 2026\n")
    f.write(f"Environment: SIT (dataadmin@leaveplus.com.au.sit)\n")
    f.write(f"Total Sample Records: {len(result['records'])}\n")
    f.write("="*80 + "\n\n")
    
    for i, rec in enumerate(result['records'], 1):
        f.write(f"ACCOUNT #{i}\n")
        f.write("-" * 80 + "\n")
        f.write(f"  External ID:             {rec.get('External_Id__c', 'N/A')}\n")
        f.write(f"  Name:                    {rec.get('Name', 'N/A')}\n")
        f.write(f"  Registered Entity Name:  {rec.get('RegisteredEntityName__c', 'N/A')}\n")
        f.write(f"  Trading As:              {rec.get('TradingAs__c', 'N/A')}\n")
        f.write(f"  ABN:                     {rec.get('ABN__c', 'N/A')}\n")
        f.write(f"  ACN:                     {rec.get('ACN__c', 'N/A')}\n")
        f.write(f"  Registration Number:     {rec.get('Registration_Number__c', 'N/A')}\n")
        f.write(f"  Employment Start Date:   {rec.get('DateEmploymentCommenced__c', 'N/A')}\n")
        f.write("\n")
    
    f.write("="*80 + "\n")
    f.write("END OF SAMPLE RECORDS\n")
    f.write("="*80 + "\n")

print(f"Sample records saved to: {output_file}")
