"""
Check Salesforce Contact Department and DepartmentGroup fields
Query field metadata and actual values
"""

import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Connect to Salesforce (use .env.sit file)
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain=os.getenv('SF_DOMAIN', 'test')
)

print("=" * 80)
print("SALESFORCE CONTACT DEPARTMENT FIELDS ANALYSIS")
print("=" * 80)

# Get Contact metadata
contact_metadata = sf.Contact.describe()

# [1] Department Field
print("\n[1] Contact.Department Field:")
print("-" * 80)

dept_field = next((f for f in contact_metadata['fields'] if f['name'] == 'Department'), None)

if dept_field:
    print(f"Field: {dept_field['name']}")
    print(f"Label: {dept_field['label']}")
    print(f"Type: {dept_field['type']}")
    print(f"Length: {dept_field.get('length', 'N/A')}")
    print(f"Updateable: {dept_field['updateable']}")
else:
    print("Department field not found")

# Query existing values
print("\n  Current Values in SF:")
query = """
    SELECT Department, COUNT(Id) cnt
    FROM Contact
    WHERE Department != null
    GROUP BY Department
    ORDER BY COUNT(Id) DESC
    LIMIT 20
"""

try:
    results = sf.query_all(query)
    if results['totalSize'] > 0:
        print(f"  Found {results['totalSize']} unique Department values:")
        for record in results['records']:
            print(f"    {record['Department']}: {record['cnt']} contacts")
    else:
        print("  No Department values in SF")
except Exception as e:
    print(f"  Error querying Department: {e}")

# Count nulls
try:
    null_query = "SELECT COUNT(Id) cnt FROM Contact WHERE Department = null"
    result = sf.query(null_query)
    print(f"  NULL Department: {result['records'][0]['cnt']:,} contacts")
except Exception as e:
    print(f"  Error counting nulls: {e}")

# [2] DepartmentGroup Field
print("\n[2] Contact.DepartmentGroup Field:")
print("-" * 80)

dept_group_field = next((f for f in contact_metadata['fields'] if f['name'] == 'DepartmentGroup__c' or f['name'] == 'DepartmentGroup'), None)

# Try both standard and custom field names
field_name = 'DepartmentGroup__c'
if not dept_group_field:
    dept_group_field = next((f for f in contact_metadata['fields'] if f['name'] == 'DepartmentGroup'), None)
    if dept_group_field:
        field_name = 'DepartmentGroup'

if dept_group_field:
    print(f"Field: {dept_group_field['name']}")
    print(f"Label: {dept_group_field['label']}")
    print(f"Type: {dept_group_field['type']}")
    print(f"Length: {dept_group_field.get('length', 'N/A')}")
    print(f"Updateable: {dept_group_field['updateable']}")
    
    if dept_group_field['type'] == 'picklist':
        print(f"\n  Picklist Values:")
        if 'picklistValues' in dept_group_field:
            for pv in dept_group_field['picklistValues']:
                active_status = "✓" if pv['active'] else "✗"
                default_status = " (default)" if pv.get('defaultValue', False) else ""
                print(f"    {active_status} {pv['label']}{default_status}")
        else:
            print("    No picklist values found")
    
    # Query existing values
    print(f"\n  Current Values in SF:")
    query = f"""
        SELECT {field_name}, COUNT(Id) cnt
        FROM Contact
        WHERE {field_name} != null
        GROUP BY {field_name}
        ORDER BY COUNT(Id) DESC
    """
    
    try:
        results = sf.query_all(query)
        if results['totalSize'] > 0:
            print(f"  Found {results['totalSize']} unique values:")
            for record in results['records']:
                value = record[field_name]
                count = record['cnt']
                print(f"    {value}: {count} contacts")
        else:
            print("  No values in SF")
    except Exception as e:
        print(f"  Error querying {field_name}: {e}")
    
    # Count nulls
    try:
        null_query = f"SELECT COUNT(Id) cnt FROM Contact WHERE {field_name} = null"
        result = sf.query(null_query)
        print(f"  NULL {field_name}: {result['records'][0]['cnt']:,} contacts")
    except Exception as e:
        print(f"  Error counting nulls: {e}")
else:
    print("DepartmentGroup field not found (tried DepartmentGroup__c and DepartmentGroup)")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
