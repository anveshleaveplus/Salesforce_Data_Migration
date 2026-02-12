"""
Verify Field Officer users exist and check API user's view permissions
"""
import os
from simple_salesforce import Salesforce
from dotenv import load_dotenv

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
print("USER VISIBILITY INVESTIGATION")
print("=" * 80)

# Try to query these specific user IDs directly
failing_user_ids = ['0059p00000OFbXh', '0059p00000OFVqX']

print("\n1. Attempting direct query by ID...")
for user_id in failing_user_ids:
    try:
        # Direct REST API query
        user = sf.User.get(user_id)
        print(f"\n✅ User {user_id} EXISTS:")
        print(f"   Name: {user['Name']}")
        print(f"   Username: {user['Username']}")
        print(f"   Email: {user['Email']}")
        print(f"   Active: {user['IsActive']}")
        print(f"   Profile: {user['ProfileId']}")
    except Exception as e:
        print(f"\n❌ User {user_id} ERROR: {e}")

# Query all recently created Field Officer users  
print("\n" + "=" * 80)
print("2. Query ALL Field Officer users (created recently)")
print("=" * 80)

query = """
    SELECT Id, Name, Username, Email, IsActive, Profile.Name, UserRole.Name, CreatedDate
    FROM User 
    WHERE Email LIKE '%.sit@leaveplus.com.au'
    ORDER BY CreatedDate DESC
    LIMIT 20
"""

result = sf.query(query)
print(f"Found {len(result['records'])} Field Officer users:\n")

for user in result['records']:
    role_name = user['UserRole']['Name'] if user.get('UserRole') else 'None'
    marker = "⚠️" if user['Id'] in failing_user_ids else "  "
    print(f"{marker} {user['Id']}: {user['Name']:25} | Active: {str(user['IsActive']):5} | Role: {role_name}")

# Check if these IDs are in our mapping file
print("\n" + "=" * 80)
print("3. Check field_officer_salesforce_mapping.csv")
print("=" * 80)

import csv
with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['Salesforce_User_Id'] in failing_user_ids:
            print(f"\n{row['Officer_Code']}:")
            print(f"  User ID: {row['Salesforce_User_Id']}")
            print(f"  Email: {row['Email']}")
            print(f"  Status: {row['Status']}")
            print(f"  Active: {row.get('Is_Active', 'Unknown')}")

# Try to query Contact with current field officer assignment to see access pattern
print("\n" + "=" * 80)
print("4. Test querying Contact with FieldOfficerAllocated__c")
print("=" * 80)

# Query a contact that should have MICHAELD assigned
test_query = """
    SELECT Id, Name, External_Id__c, FieldOfficerAllocated__c, FieldOfficerAllocated__r.Name
    FROM Contact 
    WHERE External_Id__c = '1001166'
"""

try:
    result = sf.query(test_query)
    if result['records']:
        contact = result['records'][0]
        officer = contact.get('FieldOfficerAllocated__c')
        officer_name = contact.get('FieldOfficerAllocated__r', {}).get('Name') if officer else None
        
        print(f"✅ Contact 1001166:")
        print(f"   FieldOfficerAllocated__c: {officer or 'NULL'}")
        if officer_name:
            print(f"   Field Officer Name: {officer_name}")
        else:
            print(f"   Field Officer Name: (cannot see user details)")
except Exception as e:
    print(f"❌ Query failed: {e}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print("If we can GET the user by ID but not QUERY them:")
print("  - Sharing rules are restricting visibility")
print("  - User Role hierarchy prevents seeing these users")
print("  - OWD (Organization-Wide Defaults) for User object is Private")
print("\nIf we can query them but not reference in lookup:")
print("  - Lookup filter might restrict which users can be selected")
print("  - Field-level security issue")
print("  - User visibility settings in Salesforce Setup")
print("=" * 80)
