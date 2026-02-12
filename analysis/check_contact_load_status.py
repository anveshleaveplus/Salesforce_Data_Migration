"""
Check Contact load results in Salesforce
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
    domain=os.getenv('SF_DOMAIN', 'test')
)

print("="*80)
print("Contact Load Status Check")
print("="*80)

# Get total Contact count
total_contacts = sf.query("SELECT COUNT() FROM Contact")['totalSize']
print(f"\nTotal Contacts in Salesforce: {total_contacts:,}")

# Get Contacts with External_Id__c
contacts_with_ext_id = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != null")['totalSize']
print(f"Contacts with External_Id__c: {contacts_with_ext_id:,}")

# Check for new fields
print("\nChecking new field population...")

# LanguagePreference__c
lang_populated = sf.query("SELECT COUNT() FROM Contact WHERE LanguagePreference__c != null AND External_Id__c != null")['totalSize']
print(f"  LanguagePreference__c populated: {lang_populated:,} ({lang_populated/contacts_with_ext_id*100:.1f}%)")

# Title
title_populated = sf.query("SELECT COUNT() FROM Contact WHERE Title != null AND External_Id__c != null")['totalSize']
print(f"  Title populated: {title_populated:,} ({title_populated/contacts_with_ext_id*100:.1f}%)")

# GenderIdentity
gender_populated = sf.query("SELECT COUNT() FROM Contact WHERE GenderIdentity != null AND External_Id__c != null")['totalSize']
print(f"  GenderIdentity populated: {gender_populated:,} ({gender_populated/contacts_with_ext_id*100:.1f}%)")

# MailingAddress
mailing_populated = sf.query("SELECT COUNT() FROM Contact WHERE MailingStreet != null AND External_Id__c != null")['totalSize']
print(f"  MailingAddress populated: {mailing_populated:,} ({mailing_populated/contacts_with_ext_id*100:.1f}%)")

# OtherAddress  
other_populated = sf.query("SELECT COUNT() FROM Contact WHERE OtherStreet != null AND External_Id__c != null")['totalSize']
print(f"  OtherAddress populated: {other_populated:,} ({other_populated/contacts_with_ext_id*100:.1f}%)")

# Sample records
print("\nSample Contact Records (last 5 updated):")
samples = sf.query("""
    SELECT External_Id__c, FirstName, LastName, Title, GenderIdentity,
           LanguagePreference__c, MailingCity, OtherCity
    FROM Contact 
    WHERE External_Id__c != null 
    ORDER BY LastModifiedDate DESC 
    LIMIT 5
""")

for rec in samples['records']:
    print(f"\n  {rec['External_Id__c']}: {rec['FirstName']} {rec['LastName']}")
    print(f"    Title: {rec.get('Title', 'N/A')}, Gender: {rec.get('GenderIdentity', 'N/A')}")
    print(f"    Language: {rec.get('LanguagePreference__c', 'N/A')}")
    print(f"    Mailing: {rec.get('MailingCity', 'N/A')}, Other: {rec.get('OtherCity', 'N/A')}")

print("\n" + "="*80)
