"""
Generate CSV reconciliation report for SIT accounts
"""

import os
import csv
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from datetime import datetime

# Load .env.sit
load_dotenv('.env.sit')

# Connect to Salesforce
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("\nGenerating CSV reconciliation report...")

# Get counts
oracle_count = 53857
sf_count = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null")['totalSize']

# Get field population
abn_count = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null AND ABN__c != null")['totalSize']
acn_count = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null AND ACN__c != null")['totalSize']
reg_count = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null AND RegisteredEntityName__c != null")['totalSize']
trading_count = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null AND TradingAs__c != null")['totalSize']
regnum_count = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null AND Registration_Number__c != null")['totalSize']
date_count = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null AND DateEmploymentCommenced__c != null")['totalSize']

# Check duplicates
dup_query = """
SELECT External_Id__c, COUNT(Id) cnt
FROM Account
WHERE External_Id__c != null
GROUP BY External_Id__c
HAVING COUNT(Id) > 1
"""
duplicates = sf.query_all(dup_query)['totalSize']

# Build report data
report_data = [
    {'Metric': 'Oracle Records Extracted', 'Count': oracle_count, 'Percentage': '100.0%', 'Status': 'N/A'},
    {'Metric': 'Salesforce Records Loaded', 'Count': sf_count, 'Percentage': '100.0%', 'Status': '✓ MATCH' if oracle_count == sf_count else '✗ MISMATCH'},
    {'Metric': 'Duplicate External IDs', 'Count': duplicates, 'Percentage': '0.0%', 'Status': '✓ PASS' if duplicates == 0 else '✗ FAIL'},
    {'Metric': '', 'Count': '', 'Percentage': '', 'Status': ''},
    {'Metric': 'Field Population Rates:', 'Count': '', 'Percentage': '', 'Status': ''},
    {'Metric': '  ABN__c', 'Count': abn_count, 'Percentage': f'{abn_count/sf_count*100:.1f}%', 'Status': ''},
    {'Metric': '  ACN__c', 'Count': acn_count, 'Percentage': f'{acn_count/sf_count*100:.1f}%', 'Status': ''},
    {'Metric': '  RegisteredEntityName__c', 'Count': reg_count, 'Percentage': f'{reg_count/sf_count*100:.1f}%', 'Status': ''},
    {'Metric': '  TradingAs__c', 'Count': trading_count, 'Percentage': f'{trading_count/sf_count*100:.1f}%', 'Status': ''},
    {'Metric': '  Registration_Number__c', 'Count': regnum_count, 'Percentage': f'{regnum_count/sf_count*100:.1f}%', 'Status': ''},
    {'Metric': '  DateEmploymentCommenced__c', 'Count': date_count, 'Percentage': f'{date_count/sf_count*100:.1f}%', 'Status': ''},
]

# Save to CSV
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f'test_output/sit_account_reconciliation_{timestamp}.csv'
os.makedirs('test_output', exist_ok=True)

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Metric', 'Count', 'Percentage', 'Status'])
    writer.writeheader()
    writer.writerows(report_data)

print(f"✅ CSV reconciliation saved to: {output_file}")
print(f"   Oracle: {oracle_count:,} | Salesforce: {sf_count:,} | Match: {oracle_count == sf_count}")
