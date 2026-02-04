from dotenv import load_dotenv
import os
from simple_salesforce import Salesforce

load_dotenv('.env.sit')
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

result = sf.query('SELECT COUNT() FROM Account WHERE External_Id__c != null')
print(f'Accounts with External_Id__c: {result["totalSize"]:,}')
