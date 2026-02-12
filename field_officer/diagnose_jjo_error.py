"""
Diagnose the exact error for JJO and MOB user creation
"""
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

# Get profile
profile = sf.query("SELECT Id FROM Profile WHERE Name = 'Standard User' LIMIT 1")['records'][0]

print("=" * 80)
print("DIAGNOSTIC: JJO User Creation")
print("=" * 80)

# Try with minimal data first
user_data = {
    'Username': 'john.odea.sit@leaveplus.com.au',
    'Email': 'john.odea.sit@leaveplus.com.au',
    'FirstName': 'John',
    'LastName': 'Odea',
    'Alias': 'jjo',
    'ProfileId': profile['Id'],
    'TimeZoneSidKey': 'Australia/Sydney',
    'LocaleSidKey': 'en_AU',
    'EmailEncodingKey': 'UTF-8',
    'LanguageLocaleKey': 'en_US',
    'IsActive': True
}

print("\nUser data:")
for key, value in user_data.items():
    print(f"  {key}: {value}")

print("\nAttempting to create user...")
try:
    result = sf.User.create(user_data)
    print(f"✅ SUCCESS: {result['id']}")
except Exception as e:
    print(f"❌ FAILED:")
    print(f"   Type: {type(e).__name__}")
    print(f"   Message: {str(e)}")
    
    # Try to get more details
    if hasattr(e, 'content'):
        print(f"   Content: {e.content}")
    if hasattr(e, 'status'):
        print(f"   Status: {e.status}")

# Try checking if username already exists
print("\n" + "=" * 80)
print("Checking if username already exists...")
print("=" * 80)

try:
    query = "SELECT Id, Name FROM User WHERE Username = 'john.odea.sit@leaveplus.com.au'"
    result = sf.query(query)
    if result['totalSize'] > 0:
        print(f"✅ User already exists: {result['records'][0]['Id']} - {result['records'][0]['Name']}")
    else:
        print("No existing user found")
except Exception as e:
    print(f"❌ Query failed: {e}")

# Check if there's a nickname conflict
print("\n" + "=" * 80)
print("Checking for Alias/CommunityNickname conflicts...")
print("=" * 80)

try:
    query = "SELECT Id, Name, Alias FROM User WHERE Alias = 'jjo'"
    result = sf.query(query)
    if result['totalSize'] > 0:
        print(f"⚠️  Alias 'jjo' already in use:")
        for user in result['records']:
            print(f"   {user['Id']}: {user['Name']} (Alias: {user['Alias']})")
    else:
        print("No conflict with Alias 'jjo'")
except Exception as e:
    print(f"❌ Query failed: {e}")

print("\n" + "=" * 80)
