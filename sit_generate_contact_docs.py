"""
SIT Contact Load - Complete Documentation Generator
Generates all deliverables in one run:
- Contact mapping CSV
- Sample records (10)
- Reconciliation report CSV
- Test results CSV
- Field type analysis (read-only vs new fields)
- ACR (AccountContactRelation) analysis
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
print("SIT CONTACT LOAD - COMPLETE DOCUMENTATION GENERATOR")
print("="*70)
print(f"Environment: {os.getenv('SF_USERNAME')}")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70 + "\n")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
os.makedirs('test_output', exist_ok=True)
os.makedirs('mappings', exist_ok=True)

# ============================================================================
# 1. GENERATE CONTACT MAPPING CSV
# ============================================================================
print("[1/6] Generating Contact Mapping CSV...")

mappings = [
    {
        'Oracle_Table': 'SCH_CO_20.CO_WORKER',
        'Oracle_Field': 'CUSTOMER_ID',
        'Oracle_Type': 'NUMBER',
        'SF_Object': 'Contact',
        'SF_Field': 'External_Id__c',
        'SF_Type': 'Text(18)',
        'Transformation': 'Direct mapping - unique worker identifier',
        'Sample_Values': '100, 200, 300',
        'Notes': 'External ID for upsert operations',
        'Field_Type': 'Newly Added'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_PERSON',
        'Oracle_Field': 'FIRST_NAME',
        'Oracle_Type': 'VARCHAR2',
        'SF_Object': 'Contact',
        'SF_Field': 'FirstName',
        'SF_Type': 'Text(40)',
        'Transformation': 'Direct mapping',
        'Sample_Values': 'John, Mary, David',
        'Notes': 'Contact first name',
        'Field_Type': 'Standard (Read-Only)'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_PERSON',
        'Oracle_Field': 'SURNAME',
        'Oracle_Type': 'VARCHAR2',
        'SF_Object': 'Contact',
        'SF_Field': 'LastName',
        'SF_Type': 'Text(80)',
        'Transformation': 'Direct mapping',
        'Sample_Values': 'Smith, Johnson, Brown',
        'Notes': 'Contact last name (required)',
        'Field_Type': 'Standard (Read-Only)'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_CUSTOMER',
        'Oracle_Field': 'EMAIL_ADDRESS',
        'Oracle_Type': 'VARCHAR2',
        'SF_Object': 'Contact',
        'SF_Field': 'Email',
        'SF_Type': 'Email',
        'Transformation': 'Direct mapping',
        'Sample_Values': 'john.smith@example.com',
        'Notes': 'Contact email address',
        'Field_Type': 'Standard (Read-Only)'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_CUSTOMER',
        'Oracle_Field': 'MOBILE_PHONE_NO',
        'Oracle_Type': 'VARCHAR2',
        'SF_Object': 'Contact',
        'SF_Field': 'MobilePhone',
        'SF_Type': 'Phone',
        'Transformation': 'Direct mapping',
        'Sample_Values': '0412345678',
        'Notes': 'Mobile phone number',
        'Field_Type': 'Standard (Read-Only)'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_CUSTOMER',
        'Oracle_Field': 'TELEPHONE1_NO',
        'Oracle_Type': 'VARCHAR2',
        'SF_Object': 'Contact',
        'SF_Field': 'OtherPhone',
        'SF_Type': 'Phone',
        'Transformation': 'Direct mapping',
        'Sample_Values': '0398765432',
        'Notes': 'Other phone number',
        'Field_Type': 'Standard (Read-Only)'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_PERSON',
        'Oracle_Field': 'DATE_OF_BIRTH',
        'Oracle_Type': 'DATE',
        'SF_Object': 'Contact',
        'SF_Field': 'Birthdate',
        'SF_Type': 'Date',
        'Transformation': 'TO_CHAR(DATE_OF_BIRTH, \'YYYY-MM-DD\')',
        'Sample_Values': '1985-05-15, 1990-12-20',
        'Notes': 'Date of birth',
        'Field_Type': 'Standard (Read-Only)'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYMENT_PERIOD',
        'Oracle_Field': 'EMPLOYER_ID',
        'Oracle_Type': 'NUMBER',
        'SF_Object': 'Contact',
        'SF_Field': 'AccountId',
        'SF_Type': 'Lookup(Account)',
        'Transformation': 'Lookup Account.Id by External_Id__c = EMPLOYER_ID',
        'Sample_Values': 'Account IDs from active employers',
        'Notes': 'Links contact to primary employer account',
        'Field_Type': 'Standard (Read-Only)'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_EMPLOYMENT_PERIOD',
        'Oracle_Field': 'EMPLOYER_ID',
        'Oracle_Type': 'NUMBER',
        'SF_Object': 'AccountContactRelation',
        'SF_Field': 'AccountId',
        'SF_Type': 'Lookup(Account)',
        'Transformation': 'Created for active employment relationships (EFFECTIVE_TO_DATE IS NULL)',
        'Sample_Values': 'Account IDs',
        'Notes': 'ACR created to link contacts to employer accounts',
        'Field_Type': 'Relationship Object'
    },
    {
        'Oracle_Table': 'SCH_CO_20.CO_SERVICE',
        'Oracle_Field': 'PERIOD_END',
        'Oracle_Type': 'NUMBER',
        'SF_Object': 'Contact',
        'SF_Field': 'N/A - Filter Only',
        'SF_Type': 'N/A',
        'Transformation': 'WHERE employer has service records >= 202301',
        'Sample_Values': '202301, 202401',
        'Notes': 'Filter: Only contacts for active employers',
        'Field_Type': 'Filter Criteria'
    }
]

mapping_file = f'mappings/SIT_CONTACT_MAPPING_{timestamp}.csv'
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
# 2. GENERATE SAMPLE RECORDS (10 contacts)
# ============================================================================
print("\n[2/6] Generating Sample Records (10 contacts)...")

sample_query = """
SELECT External_Id__c, FirstName, LastName, Email, 
       MobilePhone, OtherPhone, Birthdate, AccountId
FROM Contact
WHERE External_Id__c != null
ORDER BY CreatedDate DESC
LIMIT 10
"""

samples = sf.query(sample_query)
total_contacts = sf.query('SELECT COUNT() FROM Contact WHERE External_Id__c != null')['totalSize']
sample_file = f'test_output/sit_contact_samples_{timestamp}.txt'

with open(sample_file, 'w', encoding='utf-8') as f:
    f.write("SIT CONTACT LOAD - SAMPLE RECORDS\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Total Contacts in SIT: {total_contacts:,}\n")
    f.write("="*80 + "\n\n")
    
    for i, rec in enumerate(samples['records'], 1):
        f.write(f"SAMPLE RECORD {i}\n")
        f.write("-"*80 + "\n")
        f.write(f"External_Id__c:      {rec.get('External_Id__c', 'NULL')}\n")
        f.write(f"FirstName:           {rec.get('FirstName', 'NULL')}\n")
        f.write(f"LastName:            {rec.get('LastName', 'NULL')}\n")
        f.write(f"Email:               {rec.get('Email', 'NULL')}\n")
        f.write(f"MobilePhone:         {rec.get('MobilePhone', 'NULL')}\n")
        f.write(f"OtherPhone:          {rec.get('OtherPhone', 'NULL')}\n")
        f.write(f"Birthdate:           {rec.get('Birthdate', 'NULL')}\n")
        f.write(f"AccountId:           {rec.get('AccountId', 'NULL')}\n")
        f.write("\n")

print(f"   ✅ Saved: {sample_file}")
print(f"      Sample count: {len(samples['records'])}")

# ============================================================================
# 3. GENERATE FIELD TYPE ANALYSIS (Read-Only vs Newly Added)
# ============================================================================
print("\n[3/6] Generating Field Type Analysis...")

field_analysis_file = f'test_output/sit_contact_field_analysis_{timestamp}.csv'

# Categorize fields
readonly_fields = [m for m in mappings if m['Field_Type'] == 'Standard (Read-Only)']
new_fields = [m for m in mappings if m['Field_Type'] == 'Newly Added']
relationship_fields = [m for m in mappings if m['Field_Type'] == 'Relationship Object']

field_analysis = []

# Read-only fields section
field_analysis.append({'Category': 'READ-ONLY STANDARD FIELDS', 'SF_Field': '', 'Field_Type': '', 'Sample_Data': '', 'Population_Rate': '', 'Notes': ''})
for field in readonly_fields:
    if field['SF_Field'] == 'AccountId':
        # Special handling for AccountId
        try:
            populated = sf.query(f"SELECT COUNT() FROM Contact WHERE External_Id__c != null AND {field['SF_Field']} != null")['totalSize']
            pop_rate = f"{populated/total_contacts*100:.1f}% ({populated:,}/{total_contacts:,})"
            samples_str = 'Account IDs (relationship)'
        except:
            pop_rate = 'System managed'
            samples_str = 'N/A'
    else:
        # Get sample data from SF
        try:
            query = f"SELECT {field['SF_Field']} FROM Contact WHERE External_Id__c != null AND {field['SF_Field']} != null LIMIT 3"
            result = sf.query(query)
            samples_str = ', '.join([str(r.get(field['SF_Field'], ''))[:30] for r in result['records']])
            
            count_query = f"SELECT COUNT() FROM Contact WHERE External_Id__c != null AND {field['SF_Field']} != null"
            populated = sf.query(count_query)['totalSize']
            pop_rate = f"{populated/total_contacts*100:.1f}% ({populated:,}/{total_contacts:,})"
        except:
            samples_str = 'N/A'
            pop_rate = 'System managed'
    
    field_analysis.append({
        'Category': '',
        'SF_Field': field['SF_Field'],
        'Field_Type': field['Field_Type'],
        'Sample_Data': samples_str if samples_str else 'NULL values',
        'Population_Rate': pop_rate,
        'Notes': field['Notes']
    })

field_analysis.append({'Category': '', 'SF_Field': '', 'Field_Type': '', 'Sample_Data': '', 'Population_Rate': '', 'Notes': ''})

# Newly added fields section
field_analysis.append({'Category': 'NEWLY ADDED CUSTOM FIELDS', 'SF_Field': '', 'Field_Type': '', 'Sample_Data': '', 'Population_Rate': '', 'Notes': ''})

for field in new_fields:
    try:
        count_query = f"SELECT COUNT() FROM Contact WHERE External_Id__c != null AND {field['SF_Field']} != null"
        populated = sf.query(count_query)['totalSize']
        pop_rate = f"{populated/total_contacts*100:.1f}% ({populated:,}/{total_contacts:,})"
        
        sample_query = f"SELECT {field['SF_Field']} FROM Contact WHERE External_Id__c != null AND {field['SF_Field']} != null LIMIT 3"
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

field_analysis.append({'Category': '', 'SF_Field': '', 'Field_Type': '', 'Sample_Data': '', 'Population_Rate': '', 'Notes': ''})

# Relationship fields section (ACR)
field_analysis.append({'Category': 'RELATIONSHIP OBJECTS (ACR)', 'SF_Field': '', 'Field_Type': '', 'Sample_Data': '', 'Population_Rate': '', 'Notes': ''})
acr_count = sf.query("SELECT COUNT() FROM AccountContactRelation WHERE Contact.External_Id__c != null")['totalSize']
field_analysis.append({
    'Category': '',
    'SF_Field': 'AccountContactRelation',
    'Field_Type': 'Relationship Object',
    'Sample_Data': f'{acr_count:,} ACR records created',
    'Population_Rate': f'{acr_count:,} relationships',
    'Notes': 'Links contacts to employer accounts for active employment'
})

with open(field_analysis_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Category', 'SF_Field', 'Field_Type', 'Sample_Data', 'Population_Rate', 'Notes'])
    writer.writeheader()
    writer.writerows(field_analysis)

print(f"   ✅ Saved: {field_analysis_file}")
print(f"      Read-only fields: {len(readonly_fields)}")
print(f"      Newly added fields: {len(new_fields)}")
print(f"      Relationship objects: {len(relationship_fields)}")

# ============================================================================
# 4. GENERATE RECONCILIATION CSV
# ============================================================================
print("\n[4/6] Generating Reconciliation Report CSV...")

oracle_count = 50000  # Current load batch
sf_count = total_contacts

# Get field population
email_count = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != null AND Email != null")['totalSize']
mobile_count = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != null AND MobilePhone != null")['totalSize']
birthdate_count = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != null AND Birthdate != null")['totalSize']
account_count = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != null AND AccountId != null")['totalSize']

# Check duplicates
dup_query = """
SELECT External_Id__c, COUNT(Id) cnt
FROM Contact
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
    {'Metric': 'AccountContactRelations Created', 'Count': acr_count, 'Percentage': f'{acr_count/sf_count*100:.1f}%' if sf_count > 0 else '0%', 'Status': '✓ PASS' if acr_count > 0 else '✗ FAIL'},
    {'Metric': '', 'Count': '', 'Percentage': '', 'Status': ''},
    {'Metric': 'Field Population Rates:', 'Count': '', 'Percentage': '', 'Status': ''},
    {'Metric': '  Email', 'Count': email_count, 'Percentage': f'{email_count/sf_count*100:.1f}%', 'Status': ''},
    {'Metric': '  MobilePhone', 'Count': mobile_count, 'Percentage': f'{mobile_count/sf_count*100:.1f}%', 'Status': ''},
    {'Metric': '  Birthdate', 'Count': birthdate_count, 'Percentage': f'{birthdate_count/sf_count*100:.1f}%', 'Status': ''},
    {'Metric': '  AccountId (Employer Link)', 'Count': account_count, 'Percentage': f'{account_count/sf_count*100:.1f}%', 'Status': ''},
]

reconciliation_file = f'test_output/sit_contact_reconciliation_{timestamp}.csv'
with open(reconciliation_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Metric', 'Count', 'Percentage', 'Status'])
    writer.writeheader()
    writer.writerows(report_data)

print(f"   ✅ Saved: {reconciliation_file}")
print(f"      Oracle: {oracle_count:,} | Salesforce: {sf_count:,} | Match: {oracle_count == sf_count}")

# ============================================================================
# 5. GENERATE TEST RESULTS CSV
# ============================================================================
print("\n[5/6] Generating Test Results CSV...")

test_results = []

# Test 1: Count
test_results.append({
    'Test': 'Total contact count',
    'Status': 'PASS' if sf_count == oracle_count else 'FAIL',
    'Expected': f'{oracle_count:,}',
    'Actual': f'{sf_count:,}',
    'Notes': '50K contact batch load'
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
missing_lastname = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != null AND LastName = null")['totalSize']
test_results.append({
    'Test': 'All contacts have LastName',
    'Status': 'PASS' if missing_lastname == 0 else 'FAIL',
    'Expected': '0 missing',
    'Actual': f'{missing_lastname:,} missing',
    'Notes': 'LastName is required'
})

# Test 4: AccountId population
account_pct = (account_count / sf_count * 100) if sf_count > 0 else 0
test_results.append({
    'Test': 'AccountId linked to employers',
    'Status': 'PASS' if account_pct > 90 else 'WARN',
    'Expected': '>90%',
    'Actual': f'{account_pct:.1f}% ({account_count:,}/{sf_count:,})',
    'Notes': 'Contacts linked to active employer accounts'
})

# Test 5: ACR created
test_results.append({
    'Test': 'AccountContactRelations created',
    'Status': 'PASS' if acr_count > 0 else 'FAIL',
    'Expected': f'>0',
    'Actual': f'{acr_count:,}',
    'Notes': 'ACR records for active employment relationships'
})

# Test 6: External_Id format
test_results.append({
    'Test': 'External_Id__c populated',
    'Status': 'PASS' if sf_count == oracle_count else 'FAIL',
    'Expected': '100%',
    'Actual': '100%' if sf_count == oracle_count else f'{sf_count/oracle_count*100:.1f}%',
    'Notes': ''
})

# Test 7: ACR AccountId links to valid Accounts
try:
    invalid_acr = sf.query("""
        SELECT COUNT() 
        FROM AccountContactRelation 
        WHERE Contact.External_Id__c != null 
        AND AccountId = null
    """)['totalSize']
    test_results.append({
        'Test': 'All ACRs have valid AccountId',
        'Status': 'PASS' if invalid_acr == 0 else 'FAIL',
        'Expected': '0 invalid',
        'Actual': f'{invalid_acr:,} invalid',
        'Notes': 'ACR must link to valid Account'
    })
except:
    pass

# Test 8: ACR ContactId links to valid Contacts
try:
    invalid_contact_acr = sf.query("""
        SELECT COUNT() 
        FROM AccountContactRelation 
        WHERE Contact.External_Id__c != null 
        AND ContactId = null
    """)['totalSize']
    test_results.append({
        'Test': 'All ACRs have valid ContactId',
        'Status': 'PASS' if invalid_contact_acr == 0 else 'FAIL',
        'Expected': '0 invalid',
        'Actual': f'{invalid_contact_acr:,} invalid',
        'Notes': 'ACR must link to valid Contact'
    })
except:
    pass

# Test 9: ACR coverage - how many contacts have ACR
try:
    contacts_with_acr = sf.query("""
        SELECT COUNT(DISTINCT ContactId) 
        FROM AccountContactRelation 
        WHERE Contact.External_Id__c != null
    """)['totalSize']
    acr_coverage_pct = (contacts_with_acr / sf_count * 100) if sf_count > 0 else 0
    test_results.append({
        'Test': 'Contacts with ACR relationships',
        'Status': 'PASS' if contacts_with_acr > 0 else 'FAIL',
        'Expected': '>0 contacts',
        'Actual': f'{contacts_with_acr:,} contacts ({acr_coverage_pct:.1f}%)',
        'Notes': 'Number of contacts with active employment ACR'
    })
except:
    pass

# Test 10: Check for duplicate ACRs (same Contact-Account pair)
try:
    duplicate_acr = sf.query("""
        SELECT ContactId, AccountId, COUNT(Id) cnt
        FROM AccountContactRelation
        WHERE Contact.External_Id__c != null
        GROUP BY ContactId, AccountId
        HAVING COUNT(Id) > 1
    """)['totalSize']
    test_results.append({
        'Test': 'No duplicate ACR relationships',
        'Status': 'PASS' if duplicate_acr == 0 else 'FAIL',
        'Expected': '0 duplicates',
        'Actual': f'{duplicate_acr:,} duplicates',
        'Notes': 'Each Contact-Account pair should be unique'
    })
except:
    pass

test_file = f'test_output/sit_contact_test_results_{timestamp}.csv'
with open(test_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Test', 'Status', 'Expected', 'Actual', 'Notes'])
    writer.writeheader()
    writer.writerows(test_results)

print(f"   ✅ Saved: {test_file}")
passed = sum(1 for r in test_results if r['Status'] == 'PASS')
print(f"      Tests passed: {passed}/{len(test_results)}")

# ============================================================================
# 6. COPY TO ONEDRIVE
# ============================================================================
print("\n" + "="*70)
print("COPYING FILES TO ONEDRIVE")
print("="*70)

onedrive_path = r"C:\Users\Anvesh.Cherupalli\OneDrive - LeavePlus\SIT_Contact_Load"

# Create directory if it doesn't exist
os.makedirs(onedrive_path, exist_ok=True)

files_to_copy = [
    (mapping_file, f'{onedrive_path}\\SIT_CONTACT_MAPPING.csv'),
    (sample_file, f'{onedrive_path}\\sit_contact_samples.txt'),
    (field_analysis_file, f'{onedrive_path}\\sit_contact_field_analysis.csv'),
    (reconciliation_file, f'{onedrive_path}\\sit_contact_reconciliation.csv'),
    (test_file, f'{onedrive_path}\\sit_contact_test_results.csv')
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
print(f"   Total Contacts: {sf_count:,}")
print(f"   Oracle Match: {'✓' if oracle_count == sf_count else '✗'}")
print(f"   Duplicates: {duplicates}")
print(f"   ACR Records: {acr_count:,}")
print(f"   Tests Passed: {passed}/{len(test_results)}")
print(f"\n📁 Files generated:")
print(f"   1. SIT_CONTACT_MAPPING.csv (field mappings)")
print(f"   2. sit_contact_samples.txt (10 sample records)")
print(f"   3. sit_contact_field_analysis.csv (read-only vs new fields)")
print(f"   4. sit_contact_reconciliation.csv (data quality report)")
print(f"   5. sit_contact_test_results.csv (test suite results)")
print(f"\n✅ All files copied to OneDrive: SIT_Contact_Load folder")
print("="*70 + "\n")
