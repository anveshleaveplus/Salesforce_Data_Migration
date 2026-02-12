"""
Check which employer has the maximum NumberOfEmployees
"""
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import os

load_dotenv('.env.sit')

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain=os.getenv('SF_DOMAIN')
)

print("Checking employer with maximum employees...")
print("="*80)

# Get the max employee count
max_query = """
SELECT External_Id__c, Name, NumberOfEmployees
FROM Account
WHERE NumberOfEmployees != null
ORDER BY NumberOfEmployees DESC
LIMIT 10
"""

results = sf.query_all(max_query)

print("\nTop 10 Employers by Employee Count:")
print("-"*80)
print(f"{'External_Id':<15} {'Name':<50} {'Employees':<12}")
print("-"*80)

for rec in results['records']:
    ext_id = rec.get('External_Id__c', '')
    name = rec.get('Name', '')[:50]
    employees = rec.get('NumberOfEmployees', 0)
    print(f"{ext_id:<15} {name:<50} {employees:>10,}")

# Specifically check External_Id: 23000
print("\n" + "="*80)
print("Checking External_Id: 23000 specifically...")

specific_query = """
SELECT External_Id__c, Name, NumberOfEmployees
FROM Account
WHERE External_Id__c = '23000'
"""

specific = sf.query(specific_query)
if specific['totalSize'] > 0:
    rec = specific['records'][0]
    print(f"\nExternal_Id__c: {rec.get('External_Id__c')}")
    print(f"Name: {rec.get('Name')}")
    print(f"NumberOfEmployees: {rec.get('NumberOfEmployees'):,}")
else:
    print("No account found with External_Id = 23000")

print("="*80)
