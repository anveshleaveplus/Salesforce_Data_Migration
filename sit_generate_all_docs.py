"""
SIT Account Load - Complete Documentation Generator
Generates all deliverables in one run:
- Account mapping CSV
- Sample records (10)
- Reconciliation report CSV
- Test results CSV
- Field type analysis (read-only vs new fields)
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

print("\n" + "="*70)
print("SIT ACCOUNT LOAD - COMPLETE DOCUMENTATION GENERATOR")
print("="*70)
print(f"Environment: {os.getenv('SF_USERNAME')}")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70 + "\n")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
os.makedirs('test_output', exist_ok=True)
os.makedirs('mappings', exist_ok=True)

# ============================================================================
# 1. GENERATE ACCOUNT MAPPING CSV
# ============================================================================
print("[1/5] Generating Account Mapping CSV...")

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
        'Notes': 'External ID for upsert operations',
        'Field_Type': 'Newly Added'
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
        'Notes': 'Official registered name',
        'Field_Type': 'Newly Added'
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
        'Notes': 'Trading name',
        'Field_Type': 'Newly Added'
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
        'Notes': 'Composite name for display',
        'Field_Type': 'Standard (Read-Only)'
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
        'Notes': '11-digit ABN with leading zeros',
        'Field_Type': 'Newly Added'
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
        'Notes': '9-digit ACN with leading zeros',
        'Field_Type': 'Newly Added'
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
        'Notes': 'Registration number (same as External ID)',
        'Field_Type': 'Newly Added'
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
        'Notes': 'Date first employed workers',
        'Field_Type': 'Newly Added'
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
        'Notes': 'Mapped from CO_EMPLOYER_TYPE lookup',
        'Field_Type': 'Standard (Read-Only)'
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
        'Notes': 'Filter: Only employers with service records >= Jan 2023',
        'Field_Type': 'Filter Criteria'
    }
]

mapping_file = f'mappings/SIT_ACCOUNT_MAPPING_{timestamp}.csv'
with open(mapping_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'Oracle_Table', 'Oracle_Field', 'Oracle_Type',
        'SF_Object', 'SF_Field', 'SF_Type',
        'Transformation', 'Sample_Values', 'Notes', 'Field_Type'
    ])
    writer.writeheader()
    writer.writerows(mappings)

print(f"   ✅ Saved: {mapping_file}")
print(f"      Total mappings: {len(mappings)}")

# ============================================================================
# 2. GENERATE SAMPLE RECORDS (10 accounts)
# ============================================================================
print("\n[2/5] Generating Sample Records (10 accounts)...")

sample_query = """
SELECT External_Id__c, Name, ABN__c, ACN__c, 
       RegisteredEntityName__c, TradingAs__c, 
       Registration_Number__c, DateEmploymentCommenced__c, Type
FROM Account
WHERE External_Id__c != null
ORDER BY CreatedDate DESC
LIMIT 10
"""

samples = sf.query(sample_query)
sample_file = f'test_output/sit_account_samples_{timestamp}.txt'

with open(sample_file, 'w', encoding='utf-8') as f:
    f.write("SIT ACCOUNT LOAD - SAMPLE RECORDS\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Total Accounts in SIT: {sf.query('SELECT COUNT() FROM Account WHERE External_Id__c != null')['totalSize']:,}\n")
    f.write("="*80 + "\n\n")
    
    for i, rec in enumerate(samples['records'], 1):
        f.write(f"SAMPLE RECORD {i}\n")
        f.write("-"*80 + "\n")
        f.write(f"External_Id__c:               {rec.get('External_Id__c', 'NULL')}\n")
        f.write(f"Name:                         {rec.get('Name', 'NULL')}\n")
        f.write(f"ABN__c:                       {rec.get('ABN__c', 'NULL')}\n")
        f.write(f"ACN__c:                       {rec.get('ACN__c', 'NULL')}\n")
        f.write(f"RegisteredEntityName__c:      {rec.get('RegisteredEntityName__c', 'NULL')}\n")
        f.write(f"TradingAs__c:                 {rec.get('TradingAs__c', 'NULL')}\n")
        f.write(f"Registration_Number__c:       {rec.get('Registration_Number__c', 'NULL')}\n")
        f.write(f"DateEmploymentCommenced__c:   {rec.get('DateEmploymentCommenced__c', 'NULL')}\n")
        f.write(f"Type:                         {rec.get('Type', 'NULL')}\n")
        f.write("\n")

print(f"   ✅ Saved: {sample_file}")
print(f"      Sample count: {len(samples['records'])}")

# ============================================================================
# 3. GENERATE FIELD TYPE ANALYSIS (Read-Only vs Newly Added)
# ============================================================================
print("\n[3/5] Generating Field Type Analysis...")

field_analysis_file = f'test_output/sit_account_field_analysis_{timestamp}.csv'

# Categorize fields
readonly_fields = [m for m in mappings if m['Field_Type'] == 'Standard (Read-Only)']
new_fields = [m for m in mappings if m['Field_Type'] == 'Newly Added']
filter_fields = [m for m in mappings if m['Field_Type'] == 'Filter Criteria']

field_analysis = []

# Read-only fields section
field_analysis.append({'Category': 'READ-ONLY STANDARD FIELDS', 'SF_Field': '', 'Field_Type': '', 'Sample_Data': '', 'Population_Rate': '', 'Notes': ''})
for field in readonly_fields:
    # Get sample data from SF
    try:
        query = f"SELECT {field['SF_Field']} FROM Account WHERE External_Id__c != null AND {field['SF_Field']} != null LIMIT 3"
        result = sf.query(query)
        samples_str = ', '.join([str(r.get(field['SF_Field'], '')) for r in result['records']])
    except:
        samples_str = 'N/A'
    
    field_analysis.append({
        'Category': '',
        'SF_Field': field['SF_Field'],
        'Field_Type': field['Field_Type'],
        'Sample_Data': samples_str if samples_str else 'NULL values',
        'Population_Rate': 'System managed',
        'Notes': field['Notes']
    })

field_analysis.append({'Category': '', 'SF_Field': '', 'Field_Type': '', 'Sample_Data': '', 'Population_Rate': '', 'Notes': ''})

# Newly added fields section
field_analysis.append({'Category': 'NEWLY ADDED CUSTOM FIELDS', 'SF_Field': '', 'Field_Type': '', 'Sample_Data': '', 'Population_Rate': '', 'Notes': ''})

total_count = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null")['totalSize']

for field in new_fields:
    # Get population rate and samples
    try:
        count_query = f"SELECT COUNT() FROM Account WHERE External_Id__c != null AND {field['SF_Field']} != null"
        populated = sf.query(count_query)['totalSize']
        pop_rate = f"{populated/total_count*100:.1f}% ({populated:,}/{total_count:,})"
        
        sample_query = f"SELECT {field['SF_Field']} FROM Account WHERE External_Id__c != null AND {field['SF_Field']} != null LIMIT 3"
        result = sf.query(sample_query)
        samples_str = ', '.join([str(r.get(field['SF_Field'], ''))[:50] for r in result['records']])
    except Exception as e:
        pop_rate = 'Error'
        samples_str = 'N/A'
    
    field_analysis.append({
        'Category': '',
        'SF_Field': field['SF_Field'],
        'Field_Type': field['Field_Type'],
        'Sample_Data': samples_str if samples_str else 'NULL values',
        'Population_Rate': pop_rate,
        'Notes': field['Notes']
    })

with open(field_analysis_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Category', 'SF_Field', 'Field_Type', 'Sample_Data', 'Population_Rate', 'Notes'])
    writer.writeheader()
    writer.writerows(field_analysis)

print(f"   ✅ Saved: {field_analysis_file}")
print(f"      Read-only fields: {len(readonly_fields)}")
print(f"      Newly added fields: {len(new_fields)}")

# ============================================================================
# 4. GENERATE RECONCILIATION CSV
# ============================================================================
print("\n[4/5] Generating Reconciliation Report CSV...")

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

reconciliation_file = f'test_output/sit_account_reconciliation_{timestamp}.csv'
with open(reconciliation_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Metric', 'Count', 'Percentage', 'Status'])
    writer.writeheader()
    writer.writerows(report_data)

print(f"   ✅ Saved: {reconciliation_file}")
print(f"      Oracle: {oracle_count:,} | Salesforce: {sf_count:,} | Match: {oracle_count == sf_count}")

# ============================================================================
# 5. GENERATE TEST RESULTS CSV
# ============================================================================
print("\n[5/5] Generating Test Results CSV...")

test_results = []

# Test 1: Count
test_results.append({
    'Test': 'Total account count',
    'Status': 'PASS' if sf_count == oracle_count else 'FAIL',
    'Expected': f'{oracle_count:,}',
    'Actual': f'{sf_count:,}',
    'Notes': 'Active employers with service >= 2023'
})

# Test 2: Duplicates
test_results.append({
    'Test': 'No duplicate External_Id__c',
    'Status': 'PASS' if duplicates == 0 else 'FAIL',
    'Expected': '0',
    'Actual': str(duplicates),
    'Notes': ''
})

# Test 3: Required fields
missing_name = sf.query("SELECT COUNT() FROM Account WHERE External_Id__c != null AND Name = null")['totalSize']
test_results.append({
    'Test': 'All accounts have Name',
    'Status': 'PASS' if missing_name == 0 else 'FAIL',
    'Expected': '0 missing',
    'Actual': f'{missing_name:,} missing',
    'Notes': ''
})

# Test 4: ABN population
abn_pct = (abn_count / sf_count * 100) if sf_count > 0 else 0
test_results.append({
    'Test': 'ABN population rate',
    'Status': 'PASS' if abn_pct > 50 else 'WARN',
    'Expected': '>50%',
    'Actual': f'{abn_pct:.1f}% ({abn_count:,}/{sf_count:,})',
    'Notes': ''
})

# Test 5: Registration Number
test_results.append({
    'Test': 'All have Registration_Number__c',
    'Status': 'PASS' if regnum_count == sf_count else 'FAIL',
    'Expected': f'{sf_count:,}',
    'Actual': f'{regnum_count:,}',
    'Notes': ''
})

test_file = f'test_output/sit_account_test_results_{timestamp}.csv'
with open(test_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Test', 'Status', 'Expected', 'Actual', 'Notes'])
    writer.writeheader()
    writer.writerows(test_results)

print(f"   ✅ Saved: {test_file}")
passed = sum(1 for r in test_results if r['Status'] == 'PASS')
print(f"      Tests passed: {passed}/{len(test_results)}")

# ============================================================================
# COPY TO ONEDRIVE
# ============================================================================
print("\n" + "="*70)
print("COPYING FILES TO ONEDRIVE")
print("="*70)

onedrive_path = r"C:\Users\Anvesh.Cherupalli\OneDrive - LeavePlus\SIT_Account_Load"

files_to_copy = [
    (mapping_file, f'{onedrive_path}\\SIT_ACCOUNT_MAPPING.csv'),
    (sample_file, f'{onedrive_path}\\sit_account_samples.txt'),
    (field_analysis_file, f'{onedrive_path}\\sit_account_field_analysis.csv'),
    (reconciliation_file, f'{onedrive_path}\\sit_account_reconciliation.csv'),
    (test_file, f'{onedrive_path}\\sit_account_test_results.csv')
]

import shutil
for src, dst in files_to_copy:
    shutil.copy2(src, dst)
    print(f"   ✅ {os.path.basename(dst)}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("DOCUMENTATION GENERATION COMPLETE")
print("="*70)
print(f"\n📊 Summary:")
print(f"   Total Accounts: {sf_count:,}")
print(f"   Oracle Match: {'✓' if oracle_count == sf_count else '✗'}")
print(f"   Duplicates: {duplicates}")
print(f"   Tests Passed: {passed}/{len(test_results)}")
print(f"\n📁 Files generated:")
print(f"   1. SIT_ACCOUNT_MAPPING.csv (field mappings)")
print(f"   2. sit_account_samples.txt (10 sample records)")
print(f"   3. sit_account_field_analysis.csv (read-only vs new fields)")
print(f"   4. sit_account_reconciliation.csv (data quality report)")
print(f"   5. sit_account_test_results.csv (test suite results)")
print(f"\n✅ All files copied to OneDrive: SIT_Account_Load folder")
print("="*70 + "\n")
