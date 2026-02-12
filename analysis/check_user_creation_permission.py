"""
Check if current user has permission to create Salesforce users
"""

import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv()

sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain='test'
)

print("=" * 80)
print("CHECKING USER PERMISSIONS FOR USER CREATION")
print("=" * 80)

# Check current user details
current_user = sf.query("""
    SELECT Id, Username, Profile.Name, UserRole.Name, IsActive
    FROM User
    WHERE Username = '{}'
""".format(os.getenv('SF_USERNAME')))

if current_user['totalSize'] > 0:
    user = current_user['records'][0]
    print(f"\n✓ Current User: {user['Username']}")
    print(f"  Profile: {user['Profile']['Name']}")
    role_name = user['UserRole']['Name'] if user.get('UserRole') else 'N/A'
    print(f"  Role: {role_name}")
    print(f"  Active: {user['IsActive']}")
else:
    print("\n✗ Could not find current user")
    exit(1)

# Check if user has admin permissions
print("\n" + "=" * 80)
print("CHECKING ADMIN PERMISSIONS")
print("=" * 80)

# Try to query UserLicense (only admins can see all licenses)
try:
    licenses = sf.query("""
        SELECT Id, Name, Status, UsedLicenses, TotalLicenses
        FROM UserLicense
        WHERE Name LIKE '%Salesforce%'
        ORDER BY Name
    """)
    
    print(f"\n✓ Can query UserLicense ({licenses['totalSize']} licenses found)")
    print("\nAvailable Licenses:")
    for lic in licenses['records']:
        used = lic['UsedLicenses']
        total = lic['TotalLicenses']
        available = total - used
        print(f"  {lic['Name']}: {used}/{total} used, {available} available")
    
    has_admin = True
except Exception as e:
    print(f"\n✗ Cannot query UserLicense: {str(e)}")
    print("  This usually means insufficient permissions")
    has_admin = False

# Check if we can access Profile object
try:
    profiles = sf.query("""
        SELECT Id, Name, UserLicenseId
        FROM Profile
        WHERE Name IN ('Standard User', 'System Administrator')
        LIMIT 5
    """)
    print(f"\n✓ Can query Profile ({profiles['totalSize']} profiles found)")
    for prof in profiles['records']:
        print(f"  {prof['Name']} (ID: {prof['Id']})")
except Exception as e:
    print(f"\n✗ Cannot query Profile: {str(e)}")
    has_admin = False

# Final verdict
print("\n" + "=" * 80)
print("VERDICT")
print("=" * 80)

if has_admin:
    print("\n✅ YOU CAN CREATE USERS!")
    print("   Your API user has sufficient permissions")
    print("   Available licenses found")
    print("\n   Next step: Run create_field_officer_users.py")
else:
    print("\n❌ CANNOT CREATE USERS")
    print("   Your API user lacks admin permissions")
    print("\n   Options:")
    print("   1. Ask SF Admin to grant you 'Manage Users' permission")
    print("   2. Ask SF Admin to create users for you")
    print("   3. Use an admin API user account")

print("=" * 80)
