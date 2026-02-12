"""
Quick test to identify Contact load error
"""
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce
import pandas as pd

# Load environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)

# Connect to Salesforce
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    security_token=os.getenv('SF_SECURITY_TOKEN'),
    domain=os.getenv('SF_DOMAIN', 'test')
)

print("Testing Contact field permissions and data format...\n")

# Create a test record with all 21 fields
test_record = {
    'External_Id__c': 'TEST_999999',
    'FirstName': 'Test',
    'LastName': 'Worker',
    'Birthdate': '1990-01-01',
    'Email': 'test@example.com',
    'OtherPhone': '1234567890',
    'MobilePhone': '0987654321',
    'LanguagePreference__c': 'English',
    'Title': 'Mr',
    'GenderIdentity': 'Male',
    'MailingStreet': '123 Test St',
    'MailingCity': 'Sydney',
    'MailingState': 'NSW',
    'MailingPostalCode': '2000',
    'MailingCountry': 'Australia',
    'OtherStreet': '456 Other St',
    'OtherCity': 'Melbourne',
    'OtherState': 'VIC',
    'OtherPostalCode': '3000',
    'OtherCountry': 'Australia'
}

print("Attempting to create test Contact with 21 fields...")
print("Fields:", list(test_record.keys()))

try:
    result = sf.Contact.create(test_record)
    print(f"\n✓ SUCCESS: Test Contact created with ID: {result['id']}")
    print("All 21 fields are accessible and valid")
    
    # Clean up
    sf.Contact.delete(result['id'])
    print("✓ Test Contact deleted")
    
except Exception as e:
    print(f"\n✗ ERROR: {e}")
    print("\nThis is likely the same error blocking your 50K load")
    
    # Try without Title and GenderIdentity
    print("\n" + "="*80)
    print("Testing without Title and GenderIdentity fields...")
    test_record2 = {k: v for k, v in test_record.items() if k not in ['Title', 'GenderIdentity']}
    
    try:
        result2 = sf.Contact.create(test_record2)
        print(f"✓ SUCCESS without Title/TitleType: {result2['id']}")
        print("Issue is with Title or TitleType fields")
        sf.Contact.delete(result2['id'])
    except Exception as e2:
        print(f"✗ Still failed: {e2}")
