"""
Check User visibility settings for failing Field Officers
"""
import os
from simple_salesforce import Salesforce
from dotenv import load_dotenv

# Load SIT environment variables (same as sit_contact_load.py)
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

# Connect to Salesforce SIT
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("=" * 80)
print("FIELD OFFICER USER VISIBILITY CHECK")
print("=" * 80)

# The two failing users
failing_users = [
    ('MICHAELD', '0059p00000OFbXh', 11),
    ('JEREMYT', '0059p00000OFVqX', 8)
]

for officer_code, user_id, contact_count in failing_users:
    print(f"\n{officer_code} ({user_id}) - {contact_count} contacts failing")
    print("-" * 80)
    
    # Query user details
    query = f"""
        SELECT Id, Name, Username, Email, IsActive, Profile.Name, 
               UserRole.Name, ManagerId, Manager.Name
        FROM User 
        WHERE Id = '{user_id}'
    """
    
    result = sf.query(query)
    
    if result['records']:
        user = result['records'][0]
        print(f"  Name: {user['Name']}")
        print(f"  Username: {user['Username']}")
        print(f"  Email: {user['Email']}")
        print(f"  Active: {user['IsActive']}")
        print(f"  Profile: {user['Profile']['Name']}")
        
        role = user.get('UserRole')
        if role:
            print(f"  Role: {role['Name']}")
        else:
            print(f"  Role: None (NO ROLE ASSIGNED!)")
        
        manager = user.get('Manager')
        if manager:
            print(f"  Manager: {manager['Name']}")
        else:
            print(f"  Manager: None")

# Check API user details
print("\n" + "=" * 80)
print("API USER (anvesh.cherupalli@leaveplus.com.au.proto)")
print("=" * 80)

api_query = f"""
    SELECT Id, Name, Username, Profile.Name, UserRole.Name
    FROM User 
    WHERE Username = '{os.getenv('SF_USERNAME')}'
"""

api_result = sf.query(api_query)
if api_result['records']:
    api_user = api_result['records'][0]
    print(f"  Name: {api_user['Name']}")
    print(f"  Profile: {api_user['Profile']['Name']}")
    
    role = api_user.get('UserRole')
    if role:
        print(f"  Role: {role['Name']}")
    else:
        print(f"  Role: None")

# Check field-level security on FieldOfficerAllocated__c
print("\n" + "=" * 80)
print("FIELD-LEVEL SECURITY CHECK")
print("=" * 80)

# Check if we can query the field
try:
    test_query = """
        SELECT Id, Name, FieldOfficerAllocated__c 
        FROM Contact 
        WHERE External_Id__c = '1001166'
        LIMIT 1
    """
    test_result = sf.query(test_query)
    print(f"✅ Can read FieldOfficerAllocated__c field")
    
    if test_result['records']:
        contact = test_result['records'][0]
        current_value = contact.get('FieldOfficerAllocated__c')
        print(f"   Current value: {current_value or 'NULL'}")
        
except Exception as e:
    print(f"❌ Cannot read FieldOfficerAllocated__c: {e}")

print("\n" + "=" * 80)
print("DIAGNOSIS")
print("=" * 80)
print("If Field Officers have:")
print("  - No Role: User references might require role hierarchy access")
print("  - Different Profile: Sharing rules might restrict visibility")
print("  - Standard User Profile: Less visible than System Administrator")
print("\nPossible solutions:")
print("  1. Assign roles to Field Officer users")
print("  2. Add Field Officers to a Public Group, share with API user")
print("  3. Change FieldOfficerAllocated__c lookup filter settings")
print("  4. Use System Administrator profile for Field Officers (not recommended)")
print("=" * 80)
