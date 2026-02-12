"""
Check Salesforce Contact.IDStatus__c field values
Query actual values to understand what this field represents
"""

import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Load environment variables
load_dotenv()

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
print("SALESFORCE CONTACT.IDSTATUS__C FIELD VALUES ANALYSIS")
print("=" * 80)

# Query all unique IDStatus__c values with counts
print("\n[1] IDStatus__c Value Distribution:")
print("-" * 80)

query = """
    SELECT IDStatus__c, COUNT(Id) cnt
    FROM Contact
    WHERE IDStatus__c != null
    GROUP BY IDStatus__c
    ORDER BY COUNT(Id) DESC
"""

try:
    results = sf.query_all(query)
    
    if results['totalSize'] > 0:
        print(f"\nFound {results['totalSize']} unique IDStatus__c values:")
        print(f"{'Value':<40} {'Count':>10}")
        print("-" * 52)
        
        total_with_values = 0
        for record in results['records']:
            value = record['IDStatus__c']
            count = record['cnt']
            total_with_values += count
            print(f"{value:<40} {count:>10,}")
        
        print("-" * 52)
        print(f"{'Total with IDStatus__c':<40} {total_with_values:>10,}")
    else:
        print("\nNo records found with IDStatus__c values")
        
except Exception as e:
    print(f"Error querying IDStatus__c values: {e}")

# Check null values
print("\n[2] NULL Value Count:")
print("-" * 80)

query_null = "SELECT COUNT(Id) cnt FROM Contact WHERE IDStatus__c = null"

try:
    result = sf.query(query_null)
    null_count = result['records'][0]['cnt']
    print(f"Contacts with NULL IDStatus__c: {null_count:,}")
except Exception as e:
    print(f"Error counting null values: {e}")

# Get field metadata if possible
print("\n[3] Field Metadata (Picklist Values):")
print("-" * 80)

try:
    # Describe Contact object to get field metadata
    contact_metadata = sf.Contact.describe()
    
    # Find IDStatus__c field
    idstatus_field = None
    for field in contact_metadata['fields']:
        if field['name'] == 'IDStatus__c':
            idstatus_field = field
            break
    
    if idstatus_field:
        print(f"\nField: {idstatus_field['name']}")
        print(f"Label: {idstatus_field['label']}")
        print(f"Type: {idstatus_field['type']}")
        print(f"Length: {idstatus_field.get('length', 'N/A')}")
        print(f"Updateable: {idstatus_field['updateable']}")
        
        if idstatus_field['type'] == 'picklist':
            print(f"\nPicklist Values:")
            if 'picklistValues' in idstatus_field:
                for pv in idstatus_field['picklistValues']:
                    active_status = "✓" if pv['active'] else "✗"
                    default_status = " (default)" if pv.get('defaultValue', False) else ""
                    print(f"  {active_status} {pv['label']}{default_status}")
            else:
                print("  No picklist values found")
        else:
            print(f"\nField type is {idstatus_field['type']}, not picklist")
    else:
        print("IDStatus__c field not found in Contact object metadata")
        
except Exception as e:
    print(f"Error getting field metadata: {e}")

# Sample records with IDStatus__c values
print("\n[4] Sample Records with IDStatus__c:")
print("-" * 80)

query_samples = """
    SELECT Id, FirstName, LastName, IDStatus__c, Oracle_Customer_ID__c
    FROM Contact
    WHERE IDStatus__c != null
    LIMIT 20
"""

try:
    results = sf.query(query_samples)
    
    if results['totalSize'] > 0:
        print(f"\nShowing {len(results['records'])} sample records:")
        print(f"{'Oracle ID':<12} {'Name':<30} {'IDStatus__c':<30}")
        print("-" * 74)
        
        for record in results['records']:
            oracle_id = record.get('Oracle_Customer_ID__c', 'N/A')
            first_name = record.get('FirstName', '')
            last_name = record.get('LastName', '')
            name = f"{first_name} {last_name}".strip() or 'N/A'
            idstatus = record.get('IDStatus__c', 'N/A')
            
            print(f"{str(oracle_id):<12} {name:<30} {idstatus:<30}")
    else:
        print("\nNo records with IDStatus__c values found")
        
except Exception as e:
    print(f"Error querying sample records: {e}")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
