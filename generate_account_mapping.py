"""
Generate Oracle to Salesforce Account Mapping CSV
For SIT load of 53,857 active employers
"""

import csv
import os

# Define mappings
mappings = [
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYER',
        'Oracle_Field': 'CUSTOMER_ID',
        'Oracle_Type': 'NUMBER',
        'SF_Object': 'Account',
        'SF_Field': 'External_Id__c',
        'SF_Type': 'Text(18)',
        'Transformation': 'Direct mapping - unique identifier',
        'Sample_Values': '1, 1001, 1005, 10032',
        'Notes': 'External ID for upsert operations'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYER',
        'Oracle_Field': 'ORGANISATION_NAME',
        'Oracle_Type': 'VARCHAR2',
        'SF_Object': 'Account',
        'SF_Field': 'RegisteredEntityName__c',
        'SF_Type': 'Text(255)',
        'Transformation': 'Direct mapping',
        'Sample_Values': 'LeavePlus Ltd, VAN DRIEL AUST P/L',
        'Notes': 'Official registered name'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYER',
        'Oracle_Field': 'TRADING_AS_NAME',
        'Oracle_Type': 'VARCHAR2',
        'SF_Object': 'Account',
        'SF_Field': 'TradingAs__c',
        'SF_Type': 'Text(255)',
        'Transformation': 'Direct mapping',
        'Sample_Values': 'LeavePlus Ltd, VAUGHAN CONSTRUCTIONS',
        'Notes': 'Trading name'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYER',
        'Oracle_Field': 'ORGANISATION_NAME + TRADING_AS_NAME',
        'Oracle_Type': 'VARCHAR2',
        'SF_Object': 'Account',
        'SF_Field': 'Name',
        'SF_Type': 'Text(255)',
        'Transformation': 'ORGANISATION_NAME || \' - \' || TRADING_AS_NAME',
        'Sample_Values': 'LeavePlus Ltd - LeavePlus Ltd',
        'Notes': 'Composite name for display'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYER',
        'Oracle_Field': 'AUSTRALIAN_BUSINESS_NUMBER',
        'Oracle_Type': 'NUMBER',
        'SF_Object': 'Account',
        'SF_Field': 'ABN__c',
        'SF_Type': 'Text(11)',
        'Transformation': 'LPAD(AUSTRALIAN_BUSINESS_NUMBER, 11, \'0\')',
        'Sample_Values': '26004334543, 18167292408',
        'Notes': '11-digit ABN with leading zeros'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYER',
        'Oracle_Field': 'AUSTRALIAN_COMPANY_NUMBER',
        'Oracle_Type': 'NUMBER',
        'SF_Object': 'Account',
        'SF_Field': 'ACN__c',
        'SF_Type': 'Text(9)',
        'Transformation': 'LPAD(AUSTRALIAN_COMPANY_NUMBER, 9, \'0\')',
        'Sample_Values': '004334543, 45435',
        'Notes': '9-digit ACN with leading zeros'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYER',
        'Oracle_Field': 'CUSTOMER_ID',
        'Oracle_Type': 'NUMBER',
        'SF_Object': 'Account',
        'SF_Field': 'Registration_Number__c',
        'SF_Type': 'Text(18)',
        'Transformation': 'TO_CHAR(CUSTOMER_ID)',
        'Sample_Values': '1, 1001, 1005',
        'Notes': 'Registration number (same as External ID)'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYER',
        'Oracle_Field': 'FIRST_EMPLOYED_WORKERS_DATE',
        'Oracle_Type': 'DATE',
        'SF_Object': 'Account',
        'SF_Field': 'DateEmploymentCommenced__c',
        'SF_Type': 'Date',
        'Transformation': 'TO_CHAR(FIRST_EMPLOYED_WORKERS_DATE, \'YYYY-MM-DD\')',
        'Sample_Values': '1977-11-18, 1978-01-01',
        'Notes': 'Date first employed workers'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYER_TYPE',
        'Oracle_Field': 'EMPLOYER_TYPE_CODE',
        'Oracle_Type': 'VARCHAR2',
        'SF_Object': 'Account',
        'SF_Field': 'Type',
        'SF_Type': 'Picklist',
        'Transformation': 'Lookup: 01=Company, 02=Trust, 03=Sole Trader',
        'Sample_Values': 'Company, Trust, Sole Trader',
        'Notes': 'Mapped from CO_EMPLOYER_TYPE lookup'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_SERVICE',
        'Oracle_Field': 'PERIOD_END',
        'Oracle_Type': 'NUMBER',
        'SF_Object': 'Account',
        'SF_Field': 'N/A - Filter Only',
        'SF_Type': 'N/A',
        'Transformation': 'WHERE EXISTS (SELECT 1 FROM CO_SERVICE WHERE PERIOD_END >= 202301)',
        'Sample_Values': '202301, 202401',
        'Notes': 'Filter: Only employers with service records >= Jan 2023'
    }
]

# Save to CSV
timestamp = '20260204'
output_file = f'mappings/SIT_ACCOUNT_MAPPING_{timestamp}.csv'
os.makedirs('mappings', exist_ok=True)

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'Oracle_Table', 'Oracle_Field', 'Oracle_Type',
        'SF_Object', 'SF_Field', 'SF_Type',
        'Transformation', 'Sample_Values', 'Notes'
    ])
    writer.writeheader()
    writer.writerows(mappings)

print(f"✅ Account mapping saved to: {output_file}")
print(f"   Total mappings: {len(mappings)}")
