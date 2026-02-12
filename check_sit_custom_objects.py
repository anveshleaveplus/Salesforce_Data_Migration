"""
List all custom objects in Salesforce SIT environment
"""
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Load SIT environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("=" * 80)
print("SALESFORCE SIT - ALL CUSTOM OBJECTS")
print("=" * 80)
print(f"Environment: {os.getenv('SF_USERNAME')}\n")

# Query all custom objects
query = """
    SELECT QualifiedApiName, Label, PluralLabel, DeveloperName
    FROM EntityDefinition
    WHERE QualifiedApiName LIKE '%__c'
    ORDER BY Label
"""

result = sf.query(query)

print(f"Found {result['totalSize']} custom objects:\n")
print(f"{'API Name':<40} {'Label':<35} {'Plural Label'}")
print("-" * 80)

migration_related = []
other_objects = []

for record in result['records']:
    api_name = record['QualifiedApiName']
    label = record['Label']
    plural = record['PluralLabel']
    
    print(f"{api_name:<40} {label:<35} {plural}")
    
    # Check if it's related to our migration (Return, Benefit, Leave, etc.)
    migration_keywords = ['return', 'benefit', 'leave', 'employment', 'service']
    if any(keyword in api_name.lower() or keyword in label.lower() for keyword in migration_keywords):
        migration_related.append(api_name)
    else:
        other_objects.append(api_name)

print("\n" + "=" * 80)
print("MIGRATION-RELATED OBJECTS")
print("=" * 80)

if migration_related:
    print(f"Found {len(migration_related)} migration-related custom objects:")
    for obj in migration_related:
        print(f"  ✅ {obj}")
else:
    print("❌ No migration-related custom objects found (Return, Benefit, Leave, etc.)")
    print("\n⚠️  ACTION NEEDED: Ask SF Admin to create custom objects:")
    print("   - Return__c (for Returns)")
    print("   - Benefit__c (for Benefits)")
    print("   - Or whatever the correct object names should be")

if other_objects:
    print(f"\nOther custom objects ({len(other_objects)}):")
    for obj in other_objects:
        print(f"  - {obj}")

print("\n" + "=" * 80)
print("STANDARD OBJECTS (for reference)")
print("=" * 80)
print("✅ Account - Standard object (exists)")
print("✅ Contact - Standard object (exists)")
print("✅ User - Standard object (exists)")
print("✅ AccountContactRelation - Standard object (exists)")
print("=" * 80)
