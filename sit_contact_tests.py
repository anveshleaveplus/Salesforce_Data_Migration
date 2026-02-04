"""
SIT - Salesforce Contact Object Tests
Validates Contact data quality in SIT environment
Tests: count, samples, data quality, account linkage, ACR relationships
"""

import os
import sys
import csv
from datetime import datetime
from dotenv import load_dotenv
from simple_salesforce import Salesforce
import pandas as pd

# Load SIT environment
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)
print(f"Using environment: {env_file}\n")

print("="*70)
print("SIT - Contact Data Validation Tests")
print("="*70)

def connect_salesforce():
    """Connect to Salesforce SIT"""
    print("\nConnecting to Salesforce SIT...")
    sf = Salesforce(
        username=os.getenv('SF_USERNAME'),
        password=os.getenv('SF_PASSWORD'),
        security_token=os.getenv('SF_SECURITY_TOKEN'),
        domain=os.getenv('SF_DOMAIN', 'test')
    )
    print("[OK] Connected")
    return sf

def test_contact_count(sf):
    """Test 1: Verify Contact count"""
    print("\n" + "="*70)
    print("TEST 1: Contact Count")
    print("="*70)
    
    try:
        # Count Contacts with External_Id__c
        result = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != NULL")
        total_contacts = result['totalSize']
        
        print(f"Total Contacts with External_Id__c: {total_contacts:,}")
        
        expected_count = 10000
        if total_contacts == expected_count:
            status = "PASS"
            message = f"Contact count matches expected: {expected_count:,}"
        elif total_contacts > expected_count:
            status = "WARN"
            message = f"More contacts than expected ({total_contacts:,} > {expected_count:,})"
        else:
            status = "FAIL"
            message = f"Fewer contacts than expected ({total_contacts:,} < {expected_count:,})"
        
        print(f"Status: {status}")
        print(f"Result: {message}")
        
        return {
            'Test': 'Contact Count',
            'Status': status,
            'Expected': expected_count,
            'Actual': total_contacts,
            'Message': message
        }
        
    except Exception as e:
        print(f"Status: ERROR")
        print(f"Error: {e}")
        return {
            'Test': 'Contact Count',
            'Status': 'ERROR',
            'Expected': 10000,
            'Actual': 0,
            'Message': str(e)
        }

def test_sample_records(sf):
    """Test 2: Retrieve and display sample Contact records"""
    print("\n" + "="*70)
    print("TEST 2: Sample Contact Records")
    print("="*70)
    
    try:
        result = sf.query("""
            SELECT External_Id__c, FirstName, LastName, Email, 
                   AccountId, Account.Name, Birthdate, 
                   OtherPhone, MobilePhone
            FROM Contact 
            WHERE External_Id__c != NULL 
            ORDER BY CreatedDate DESC 
            LIMIT 5
        """)
        
        records = result['records']
        print(f"Retrieved {len(records)} sample Contact records:\n")
        
        for i, rec in enumerate(records, 1):
            account_name = rec.get('Account', {}).get('Name', 'N/A') if rec.get('Account') else 'N/A'
            print(f"  {i}. External ID: {rec.get('External_Id__c', 'N/A')}")
            print(f"     Name: {rec.get('FirstName', '')} {rec.get('LastName', 'N/A')}")
            print(f"     Email: {rec.get('Email', 'N/A')}")
            print(f"     Account: {account_name}")
            print(f"     Birthdate: {rec.get('Birthdate', 'N/A')}")
            print(f"     Phones: {rec.get('OtherPhone', 'N/A')} / {rec.get('MobilePhone', 'N/A')}")
            print()
        
        status = "PASS" if len(records) == 5 else "WARN"
        message = f"Retrieved {len(records)}/5 sample records"
        
        print(f"Status: {status}")
        
        return {
            'Test': 'Sample Records',
            'Status': status,
            'Expected': 5,
            'Actual': len(records),
            'Message': message
        }
        
    except Exception as e:
        print(f"Status: ERROR")
        print(f"Error: {e}")
        return {
            'Test': 'Sample Records',
            'Status': 'ERROR',
            'Expected': 5,
            'Actual': 0,
            'Message': str(e)
        }

def test_data_quality(sf):
    """Test 3: Check Contact data quality"""
    print("\n" + "="*70)
    print("TEST 3: Contact Data Quality")
    print("="*70)
    
    results = []
    
    try:
        # Check for missing FirstName or LastName
        result = sf.query("""
            SELECT COUNT() 
            FROM Contact 
            WHERE External_Id__c != NULL 
            AND (FirstName = NULL OR LastName = NULL)
        """)
        missing_names = result['totalSize']
        print(f"Contacts with missing names: {missing_names:,}")
        
        # Check Email population
        result = sf.query("""
            SELECT COUNT() 
            FROM Contact 
            WHERE External_Id__c != NULL AND Email != NULL
        """)
        with_email = result['totalSize']
        print(f"Contacts with Email: {with_email:,}")
        
        # Check Phone population
        result = sf.query("""
            SELECT COUNT() 
            FROM Contact 
            WHERE External_Id__c != NULL 
            AND (OtherPhone != NULL OR MobilePhone != NULL)
        """)
        with_phone = result['totalSize']
        print(f"Contacts with at least one Phone: {with_phone:,}")
        
        # Check Birthdate population
        result = sf.query("""
            SELECT COUNT() 
            FROM Contact 
            WHERE External_Id__c != NULL AND Birthdate != NULL
        """)
        with_birthdate = result['totalSize']
        print(f"Contacts with Birthdate: {with_birthdate:,}")
        
        # Overall status
        if missing_names == 0:
            status = "PASS"
            message = "All contacts have complete names"
        else:
            status = "FAIL"
            message = f"{missing_names} contacts missing names"
        
        print(f"\nStatus: {status}")
        print(f"Result: {message}")
        
        return {
            'Test': 'Data Quality',
            'Status': status,
            'Expected': '0 missing names',
            'Actual': f'{missing_names} missing, {with_email} emails, {with_phone} phones, {with_birthdate} birthdates',
            'Message': message
        }
        
    except Exception as e:
        print(f"Status: ERROR")
        print(f"Error: {e}")
        return {
            'Test': 'Data Quality',
            'Status': 'ERROR',
            'Expected': '0 missing names',
            'Actual': 0,
            'Message': str(e)
        }

def test_account_linkage(sf):
    """Test 4: Verify Account linkage"""
    print("\n" + "="*70)
    print("TEST 4: Account Linkage")
    print("="*70)
    
    try:
        # Count Contacts with AccountId
        result = sf.query("""
            SELECT COUNT() 
            FROM Contact 
            WHERE External_Id__c != NULL AND AccountId != NULL
        """)
        linked_contacts = result['totalSize']
        
        # Count total Contacts
        result = sf.query("SELECT COUNT() FROM Contact WHERE External_Id__c != NULL")
        total_contacts = result['totalSize']
        
        linkage_pct = (linked_contacts / total_contacts * 100) if total_contacts > 0 else 0
        
        print(f"Contacts linked to Accounts: {linked_contacts:,} / {total_contacts:,} ({linkage_pct:.2f}%)")
        
        # Check distribution across Accounts
        result = sf.query("""
            SELECT Account.Name, COUNT(Id) contact_count 
            FROM Contact 
            WHERE External_Id__c != NULL AND AccountId != NULL 
            GROUP BY Account.Name 
            ORDER BY COUNT(Id) DESC 
            LIMIT 5
        """)
        
        if result['records']:
            print(f"\nTop 5 Accounts by Contact count:")
            for rec in result['records']:
                print(f"  - {rec['Name']}: {rec['contact_count']} contacts")
        
        if linkage_pct >= 95:
            status = "PASS"
            message = f"{linkage_pct:.2f}% of contacts linked to Accounts"
        elif linkage_pct >= 80:
            status = "WARN"
            message = f"Only {linkage_pct:.2f}% of contacts linked"
        else:
            status = "FAIL"
            message = f"Low linkage: {linkage_pct:.2f}%"
        
        print(f"\nStatus: {status}")
        print(f"Result: {message}")
        
        return {
            'Test': 'Account Linkage',
            'Status': status,
            'Expected': '95%+ linked',
            'Actual': f'{linked_contacts}/{total_contacts} ({linkage_pct:.2f}%)',
            'Message': message
        }
        
    except Exception as e:
        print(f"Status: ERROR")
        print(f"Error: {e}")
        return {
            'Test': 'Account Linkage',
            'Status': 'ERROR',
            'Expected': '95%+ linked',
            'Actual': 0,
            'Message': str(e)
        }

def test_acr_relationships(sf):
    """Test 5: Verify AccountContactRelation"""
    print("\n" + "="*70)
    print("TEST 5: AccountContactRelation (ACR)")
    print("="*70)
    
    try:
        # Count direct ACRs
        result = sf.query("""
            SELECT COUNT() 
            FROM AccountContactRelation 
            WHERE Contact.External_Id__c != NULL AND IsDirect = true
        """)
        direct_acr_count = result['totalSize']
        
        # Count Contacts with AccountId
        result = sf.query("""
            SELECT COUNT() 
            FROM Contact 
            WHERE External_Id__c != NULL AND AccountId != NULL
        """)
        linked_contacts = result['totalSize']
        
        print(f"Direct ACR count: {direct_acr_count:,}")
        print(f"Contacts with AccountId: {linked_contacts:,}")
        
        # Sample ACR records
        result = sf.query("""
            SELECT Contact.External_Id__c, Contact.Name, 
                   Account.Name, IsDirect, IsActive 
            FROM AccountContactRelation 
            WHERE Contact.External_Id__c != NULL 
            ORDER BY CreatedDate DESC 
            LIMIT 5
        """)
        
        if result['records']:
            print(f"\nSample ACR records:")
            for rec in result['records']:
                contact_name = rec.get('Contact', {}).get('Name', 'N/A')
                account_name = rec.get('Account', {}).get('Name', 'N/A')
                is_direct = rec.get('IsDirect', False)
                is_active = rec.get('IsActive', False)
                print(f"  - Contact: {contact_name}, Account: {account_name}, " +
                      f"Direct: {is_direct}, Active: {is_active}")
        
        # Validate: Each Contact with AccountId should have exactly 1 direct ACR
        if direct_acr_count >= linked_contacts:
            status = "PASS"
            message = f"Each linked Contact has direct ACR ({direct_acr_count} ACRs for {linked_contacts} contacts)"
        else:
            status = "FAIL"
            message = f"Missing ACRs: {direct_acr_count} ACRs < {linked_contacts} contacts"
        
        print(f"\nStatus: {status}")
        print(f"Result: {message}")
        
        return {
            'Test': 'ACR Relationships',
            'Status': status,
            'Expected': f'{linked_contacts} direct ACRs',
            'Actual': f'{direct_acr_count} direct ACRs',
            'Message': message
        }
        
    except Exception as e:
        print(f"Status: ERROR")
        print(f"Error: {e}")
        return {
            'Test': 'ACR Relationships',
            'Status': 'ERROR',
            'Expected': 'N/A',
            'Actual': 0,
            'Message': str(e)
        }

def save_results(results):
    """Save test results to CSV"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'test_output/sit_contact_test_results_{timestamp}.csv'
    
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False)
    
    print(f"\n{'='*70}")
    print(f"Test results saved to: {output_file}")
    print(f"{'='*70}")
    
    return output_file

def main():
    """Run all tests"""
    try:
        sf = connect_salesforce()
        
        results = []
        results.append(test_contact_count(sf))
        results.append(test_sample_records(sf))
        results.append(test_data_quality(sf))
        results.append(test_account_linkage(sf))
        results.append(test_acr_relationships(sf))
        
        # Save results
        output_file = save_results(results)
        
        # Summary
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        
        pass_count = sum(1 for r in results if r['Status'] == 'PASS')
        warn_count = sum(1 for r in results if r['Status'] == 'WARN')
        fail_count = sum(1 for r in results if r['Status'] == 'FAIL')
        error_count = sum(1 for r in results if r['Status'] == 'ERROR')
        
        for result in results:
            status_symbol = {
                'PASS': '✓',
                'WARN': '⚠',
                'FAIL': '✗',
                'ERROR': '!'
            }.get(result['Status'], '?')
            
            print(f"{status_symbol} {result['Test']}: {result['Status']}")
        
        print(f"\nPASS: {pass_count}, WARN: {warn_count}, FAIL: {fail_count}, ERROR: {error_count}")
        print(f"\n[OK] All tests completed")
        
    except Exception as e:
        print(f"\n[ERROR] Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
