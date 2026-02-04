"""
Basic Data Validation Tests for SIT Environment - ACCOUNT Load
Validates loaded Account data quality and completeness
"""

import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from simple_salesforce import Salesforce

# Load SIT environment
load_dotenv('.env.sit')

# Results tracking
test_results = []


def test_account_count(sf):
    """Test 1: Verify Account Records Loaded"""
    print("\n" + "="*70)
    print("TEST 1: Account Record Count")
    print("="*70)
    
    try:
        # Count accounts with External_Id__c (our loaded records)
        query = "SELECT COUNT() FROM Account WHERE External_Id__c != null"
        result = sf.query(query)
        count = result['totalSize']
        
        print(f"✅ Found {count:,} accounts with External_Id__c")
        
        if count >= 10000:
            print("✅ Expected 10,000 records - PASS")
            status = "PASS"
        else:
            print(f"⚠️  Expected 10,000 records, found {count:,}")
            status = "WARNING"
        
        test_results.append({
            'test': 'Account Count',
            'status': status,
            'value': count,
            'detail': f'{count:,} accounts loaded'
        })
        
        return count
        
    except Exception as e:
        print(f"❌ Account count query failed: {e}")
        test_results.append({
            'test': 'Account Count',
            'status': 'FAIL',
            'value': 0,
            'detail': str(e)
        })
        return 0


def test_sample_records(sf):
    """Test 2: Query Sample Records"""
    print("\n" + "="*70)
    print("TEST 2: Sample Record Data Quality")
    print("="*70)
    
    try:
        query = """
            SELECT External_Id__c, Name, ABN__c, ACN__c, Phone, 
                   Registration_Number__c, DateEmploymentCommenced__c
            FROM Account 
            WHERE External_Id__c != null 
            ORDER BY External_Id__c 
            LIMIT 5
        """
        result = sf.query(query)
        records = result['records']
        
        print(f"✅ Retrieved {len(records)} sample records:\n")
        
        for i, rec in enumerate(records, 1):
            print(f"  Record {i}:")
            print(f"    External_Id: {rec['External_Id__c']}")
            print(f"    Name: {rec['Name']}")
            print(f"    ABN: {rec.get('ABN__c', 'NULL')}")
            print(f"    ACN: {rec.get('ACN__c', 'NULL')}")
            print(f"    Phone: {rec.get('Phone', 'NULL')}")
            print(f"    Reg#: {rec.get('Registration_Number__c', 'NULL')}")
            print(f"    Start Date: {rec.get('DateEmploymentCommenced__c', 'NULL')}")
            print()
        
        test_results.append({
            'test': 'Sample Records',
            'status': 'PASS',
            'value': len(records),
            'detail': f'Retrieved {len(records)} sample records'
        })
        
        return True
        
    except Exception as e:
        print(f"❌ Sample record query failed: {e}")
        test_results.append({
            'test': 'Sample Records',
            'status': 'FAIL',
            'value': 0,
            'detail': str(e)
        })
        return False


def test_data_quality(sf):
    """Test 3: Data Quality Checks"""
    print("\n" + "="*70)
    print("TEST 3: Data Quality Validation")
    print("="*70)
    
    try:
        # Check for missing required fields
        checks = [
            ("Name", "SELECT COUNT() FROM Account WHERE External_Id__c != null AND (Name = null OR Name = '')"),
            ("ABN populated", "SELECT COUNT() FROM Account WHERE External_Id__c != null AND ABN__c != null"),
            ("ACN populated", "SELECT COUNT() FROM Account WHERE External_Id__c != null AND ACN__c != null"),
        ]
        
        for field_name, query in checks:
            result = sf.query(query)
            count = result['totalSize']
            
            if "populated" in field_name:
                print(f"✅ {field_name}: {count:,} records")
                status = "PASS"
                detail = f"{count:,} records populated"
            else:
                if count == 0:
                    print(f"✅ {field_name}: No missing values")
                    status = "PASS"
                    detail = "No missing values"
                else:
                    print(f"⚠️  {field_name}: {count:,} records missing")
                    status = "WARNING"
                    detail = f"{count:,} records missing"
            
            test_results.append({
                'test': f'Data Quality - {field_name}',
                'status': status,
                'value': count,
                'detail': detail
            })
        
        return True
        
    except Exception as e:
        print(f"❌ Data quality check failed: {e}")
        test_results.append({
            'test': 'Data Quality',
            'status': 'FAIL',
            'value': 0,
            'detail': str(e)
        })
        return False


def test_picklist_fields(sf):
    """Test 4: Verify Picklist Fields Are Empty (as expected)"""
    print("\n" + "="*70)
    print("TEST 4: Picklist Fields (Should be empty)")
    print("="*70)
    
    try:
        query = """
            SELECT AccountSubStatus__c, BusinessEntityType__c, 
                   CoverageDeterminationStatus__c, Registration_Status__c
            FROM Account 
            WHERE External_Id__c != null 
            LIMIT 1
        """
        result = sf.query(query)
        
        if result['totalSize'] > 0:
            rec = result['records'][0]
            
            picklist_fields = {
                'AccountSubStatus__c': rec.get('AccountSubStatus__c'),
                'BusinessEntityType__c': rec.get('BusinessEntityType__c'),
                'CoverageDeterminationStatus__c': rec.get('CoverageDeterminationStatus__c'),
                'Registration_Status__c': rec.get('Registration_Status__c')
            }
            
            all_empty = all(v is None for v in picklist_fields.values())
            
            if all_empty:
                print("✅ All picklist fields are NULL (as expected - skipped during load)")
                status = "PASS"
                detail = "All picklist fields NULL as expected"
            else:
                print("⚠️  Some picklist fields have values:")
                populated = []
                for field, value in picklist_fields.items():
                    if value:
                        print(f"    {field}: {value}")
                        populated.append(field)
                status = "WARNING"
                detail = f"Fields populated: {', '.join(populated)}"
            
            test_results.append({
                'test': 'Picklist Fields',
                'status': status,
                'value': 4 - sum(1 for v in picklist_fields.values() if v),
                'detail': detail
            })
        
        return True
        
    except Exception as e:
        print(f"❌ Picklist field check failed: {e}")
        test_results.append({
            'test': 'Picklist Fields',
            'status': 'FAIL',
            'value': 0,
            'detail': str(e)
        })
        return False


def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("SIT ENVIRONMENT - ACCOUNT DATA VALIDATION")
    print("="*70)
    print(f"Environment: {os.getenv('SF_USERNAME')}")
    print(f"Domain: {os.getenv('SF_DOMAIN')}")
    
    # Connect to Salesforce
    try:
        sf = Salesforce(
            username=os.getenv('SF_USERNAME'),
            password=os.getenv('SF_PASSWORD'),
            security_token=os.getenv('SF_SECURITY_TOKEN'),
            domain=os.getenv('SF_DOMAIN')
        )
        print(f"Connected to: {sf.sf_instance}\n")
    except Exception as e:
        print(f"\n❌ Cannot connect to Salesforce: {e}")
        return
    
    # Run data validation tests
    test_account_count(sf)
    test_sample_records(sf)
    test_data_quality(sf)
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUITE COMPLETE")
    print("="*70)
    
    # Count test results
    total_tests = len(test_results)
    passed = sum(1 for r in test_results if r['status'] == 'PASS')
    warnings = sum(1 for r in test_results if r['status'] == 'WARNING')
    failed = sum(1 for r in test_results if r['status'] == 'FAIL')
    
    print(f"Total Tests: {total_tests}")
    print(f"✅ Passed: {passed}")
    if warnings > 0:
        print(f"⚠️  Warnings: {warnings}")
    if failed > 0:
        print(f"❌ Failed: {failed}")
    
    # Save results to files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save CSV summary
    csv_file = f'test_output/sit_account_test_results_{timestamp}.csv'
    with open(csv_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['test', 'status', 'value', 'detail'])
        writer.writeheader()
        writer.writerows(test_results)
    
    print(f"\n📊 Results saved:")
    print(f"   CSV: {csv_file}")
    print()


if __name__ == "__main__":
    main()
