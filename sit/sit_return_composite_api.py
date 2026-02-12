"""
SIT - Salesforce Return__c and ServiceReport__c Composite API Operations
Demonstrates CREATE, UPDATE (UPSERT), READ, and DELETE operations
Uses External IDs for relationships in a single Composite request
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from simple_salesforce import Salesforce
import requests

# Load SIT environment variables
env_file = '.env.sit' if os.path.exists('.env.sit') else '.env'
load_dotenv(env_file)
print(f"Using environment: {env_file}\n")

print("="*70)
print("Salesforce Composite API - Return__c & ServiceReport__c Operations")
print("="*70)
print()

def connect_salesforce():
    """Establish Salesforce connection"""
    print("Connecting to Salesforce...")
    try:
        sf = Salesforce(
            username=os.getenv('SF_USERNAME'),
            password=os.getenv('SF_PASSWORD'),
            security_token=os.getenv('SF_SECURITY_TOKEN'),
            domain=os.getenv('SF_DOMAIN', 'test')
        )
        print(f"  [OK] Connected to: {sf.sf_instance}")
        print(f"  API Version: {sf.sf_version}\n")
        return sf
    except Exception as e:
        print(f"  [ERROR] Connection failed: {e}")
        exit(1)

def build_composite_request_upsert_and_create():
    """
    Example 1: UPSERT Return__c and CREATE child ServiceReport__c records
    Uses External IDs for Account lookup and referencing Return in child records
    """
    
    composite_request = {
        "allOrNone": False,  # Continue processing even if some fail
        "compositeRequest": [
            # Step 1: UPSERT Return__c record (using External_Id__c)
            {
                "method": "PATCH",
                "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_100001",
                "referenceId": "return1",
                "body": {
                    "External_Id__c": "WSR_100001",
                    "Employer__r": {"External_Id__c": "12345"},  # Link to Account by External ID
                    "ReturnSubmittedDate__c": "2026-01-15",
                    "TotalDaysWorked__c": 120,
                    "TotalWagesReported__c": 150000.00,
                    "Charges__c": 3500.00,
                    "ReturnType__c": "Quarterly Return"
                }
            },
            
            # Step 2: CREATE ServiceReport__c child record 1 (references return1)
            {
                "method": "POST",
                "url": "/services/data/v59.0/sobjects/ServiceReport__c",
                "referenceId": "service1",
                "body": {
                    "External_Id__c": "SR_100001_01",
                    "Return__c": "@{return1.id}",  # Reference parent Return from step 1
                    "Worker__r": {"External_Id__c": "WORKER_9001"},  # Link to Contact by External ID
                    "ServicePeriod__c": "202601",
                    "DaysWorked__c": 30,
                    "WagesEarned__c": 7500.00
                }
            },
            
            # Step 3: CREATE ServiceReport__c child record 2
            {
                "method": "POST",
                "url": "/services/data/v59.0/sobjects/ServiceReport__c",
                "referenceId": "service2",
                "body": {
                    "External_Id__c": "SR_100001_02",
                    "Return__c": "@{return1.id}",  # Reference parent Return from step 1
                    "Worker__r": {"External_Id__c": "WORKER_9002"},
                    "ServicePeriod__c": "202601",
                    "DaysWorked__c": 25,
                    "WagesEarned__c": 6250.00
                }
            }
        ]
    }
    
    return composite_request

def build_composite_request_update_and_read():
    """
    Example 2: UPDATE existing Return__c and READ its child ServiceReport__c records
    """
    
    composite_request = {
        "allOrNone": False,
        "compositeRequest": [
            # Step 1: UPDATE Return__c (using External ID)
            {
                "method": "PATCH",
                "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_100001",
                "referenceId": "returnUpdate",
                "body": {
                    "TotalDaysWorked__c": 150,  # Update days
                    "TotalWagesReported__c": 187500.00,  # Update wages
                    "Charges__c": 4375.00,  # Update charges
                    "Interest__c": 125.50  # Add interest
                }
            },
            
            # Step 2: READ the Return__c record to get its Salesforce ID
            {
                "method": "GET",
                "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_100001",
                "referenceId": "returnRead"
            },
            
            # Step 3: QUERY child ServiceReport__c records
            # Note: We'll use the ID from returnRead response
            {
                "method": "GET",
                "url": "/services/data/v59.0/query?q=SELECT+Id,External_Id__c,DaysWorked__c,WagesEarned__c+FROM+ServiceReport__c+WHERE+Return__r.External_Id__c='WSR_100001'",
                "referenceId": "serviceReportsRead"
            }
        ]
    }
    
    return composite_request

def build_composite_request_upsert_multiple_returns():
    """
    Example 3: UPSERT multiple Return__c records with different External IDs
    """
    
    composite_request = {
        "allOrNone": False,
        "compositeRequest": []
    }
    
    # UPSERT 5 Return__c records
    returns_data = [
        {
            "external_id": "WSR_100001",
            "employer_id": "12345",
            "date": "2026-01-15",
            "days": 120,
            "wages": 150000.00,
            "charges": 3500.00
        },
        {
            "external_id": "WSR_100002",
            "employer_id": "12346",
            "date": "2026-01-20",
            "days": 95,
            "wages": 118750.00,
            "charges": 2775.00
        },
        {
            "external_id": "WSR_100003",
            "employer_id": "12347",
            "date": "2026-01-25",
            "days": 200,
            "wages": 250000.00,
            "charges": 5850.00
        },
        {
            "external_id": "WSR_100004",
            "employer_id": "12348",
            "date": "2026-02-01",
            "days": 80,
            "wages": 100000.00,
            "charges": 2340.00
        },
        {
            "external_id": "WSR_100005",
            "employer_id": "12349",
            "date": "2026-02-05",
            "days": 150,
            "wages": 187500.00,
            "charges": 4375.00
        }
    ]
    
    for idx, return_data in enumerate(returns_data):
        composite_request["compositeRequest"].append({
            "method": "PATCH",
            "url": f"/services/data/v59.0/sobjects/Return__c/External_Id__c/{return_data['external_id']}",
            "referenceId": f"return{idx+1}",
            "body": {
                "External_Id__c": return_data["external_id"],
                "Employer__r": {"External_Id__c": return_data["employer_id"]},
                "ReturnSubmittedDate__c": return_data["date"],
                "TotalDaysWorked__c": return_data["days"],
                "TotalDaysReported__c": return_data["days"],
                "TotalWagesReported__c": return_data["wages"],
                "Charges__c": return_data["charges"],
                "ReturnType__c": "Quarterly Return"
            }
        })
    
    return composite_request

def build_composite_request_delete():
    """
    Example 4: DELETE ServiceReport__c records then Return__c record
    Must delete children before parent to maintain referential integrity
    """
    
    composite_request = {
        "allOrNone": True,  # All or nothing for deletes
        "compositeRequest": [
            # Step 1: Query to get ServiceReport__c IDs to delete
            {
                "method": "GET",
                "url": "/services/data/v59.0/query?q=SELECT+Id+FROM+ServiceReport__c+WHERE+Return__r.External_Id__c='WSR_100001'",
                "referenceId": "queryServiceReports"
            },
            
            # Step 2-N: DELETE individual ServiceReport__c records
            # Note: In practice, you'd need to know the IDs or query first
            {
                "method": "DELETE",
                "url": "/services/data/v59.0/sobjects/ServiceReport__c/External_Id__c/SR_100001_01",
                "referenceId": "deleteService1"
            },
            {
                "method": "DELETE",
                "url": "/services/data/v59.0/sobjects/ServiceReport__c/External_Id__c/SR_100001_02",
                "referenceId": "deleteService2"
            },
            
            # Final Step: DELETE parent Return__c record
            {
                "method": "DELETE",
                "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_100001",
                "referenceId": "deleteReturn"
            }
        ]
    }
    
    return composite_request

def build_composite_request_complete_workflow():
    """
    Example 5: Complete workflow - UPSERT Return, CREATE children, READ back, UPDATE
    """
    
    composite_request = {
        "allOrNone": False,
        "compositeRequest": [
            # 1. UPSERT Return__c
            {
                "method": "PATCH",
                "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_200001",
                "referenceId": "return1",
                "body": {
                    "External_Id__c": "WSR_200001",
                    "Employer__r": {"External_Id__c": "50000"},
                    "ReturnSubmittedDate__c": "2026-02-10",
                    "TotalDaysWorked__c": 0,  # Will calculate from children
                    "TotalWagesReported__c": 0.00,  # Will calculate from children
                    "ReturnType__c": "Monthly Return"
                }
            },
            
            # 2-6. CREATE 5 ServiceReport__c children
            {
                "method": "POST",
                "url": "/services/data/v59.0/sobjects/ServiceReport__c",
                "referenceId": "service1",
                "body": {
                    "External_Id__c": "SR_200001_01",
                    "Return__c": "@{return1.id}",
                    "Worker__r": {"External_Id__c": "WORKER_5001"},
                    "ServicePeriod__c": "202602",
                    "DaysWorked__c": 22,
                    "WagesEarned__c": 5500.00
                }
            },
            {
                "method": "POST",
                "url": "/services/data/v59.0/sobjects/ServiceReport__c",
                "referenceId": "service2",
                "body": {
                    "External_Id__c": "SR_200001_02",
                    "Return__c": "@{return1.id}",
                    "Worker__r": {"External_Id__c": "WORKER_5002"},
                    "ServicePeriod__c": "202602",
                    "DaysWorked__c": 20,
                    "WagesEarned__c": 5000.00
                }
            },
            {
                "method": "POST",
                "url": "/services/data/v59.0/sobjects/ServiceReport__c",
                "referenceId": "service3",
                "body": {
                    "External_Id__c": "SR_200001_03",
                    "Return__c": "@{return1.id}",
                    "Worker__r": {"External_Id__c": "WORKER_5003"},
                    "ServicePeriod__c": "202602",
                    "DaysWorked__c": 18,
                    "WagesEarned__c": 4500.00
                }
            },
            
            # 7. QUERY all children to calculate totals
            {
                "method": "GET",
                "url": "/services/data/v59.0/query?q=SELECT+SUM(DaysWorked__c),SUM(WagesEarned__c)+FROM+ServiceReport__c+WHERE+Return__r.External_Id__c='WSR_200001'",
                "referenceId": "queryTotals"
            },
            
            # 8. UPDATE Return__c with calculated totals
            # Note: In practice, you'd use the query results in a subsequent request
            {
                "method": "PATCH",
                "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_200001",
                "referenceId": "returnUpdate",
                "body": {
                    "TotalDaysWorked__c": 60,  # Sum from children
                    "TotalWagesReported__c": 15000.00,  # Sum from children
                    "Charges__c": 350.00
                }
            }
        ]
    }
    
    return composite_request

def execute_composite_request(sf, composite_request, description=""):
    """Execute a Composite API request and display results"""
    
    print(f"\n{'='*70}")
    print(f"Executing: {description}")
    print(f"{'='*70}")
    
    # Build the Composite API endpoint URL
    composite_url = f"https://{sf.sf_instance}/services/data/v59.0/composite"
    
    # Set headers
    headers = {
        "Authorization": f"Bearer {sf.session_id}",
        "Content-Type": "application/json"
    }
    
    # Print request summary
    print(f"\nComposite Request Summary:")
    print(f"  Operations: {len(composite_request['compositeRequest'])}")
    print(f"  All or None: {composite_request.get('allOrNone', False)}")
    
    for idx, operation in enumerate(composite_request['compositeRequest'], 1):
        print(f"  {idx}. {operation['method']} - {operation['referenceId']}")
    
    # Execute request
    try:
        print(f"\nSending request to: {composite_url}")
        response = requests.post(
            composite_url,
            headers=headers,
            json=composite_request,
            timeout=60
        )
        
        # Check response status
        if response.status_code == 200:
            print(f"  [OK] Status: {response.status_code}")
            
            result = response.json()
            
            # Display results
            print(f"\nResults:")
            for idx, sub_response in enumerate(result.get('compositeResponse', []), 1):
                ref_id = sub_response.get('referenceId')
                http_status = sub_response.get('httpStatusCode')
                body = sub_response.get('body')
                
                # Determine success/failure
                success = http_status in [200, 201, 204]
                status_icon = "[OK]" if success else "[ERROR]"
                
                print(f"\n  {idx}. {ref_id}: {status_icon} HTTP {http_status}")
                
                if success:
                    if http_status == 204:
                        print(f"     → Successfully deleted")
                    elif isinstance(body, dict):
                        if 'id' in body:
                            print(f"     → ID: {body['id']}")
                        if 'created' in body:
                            print(f"     → Created: {body['created']}")
                        if 'success' in body:
                            print(f"     → Success: {body['success']}")
                        if 'totalSize' in body:
                            print(f"     → Records returned: {body['totalSize']}")
                            if body.get('records'):
                                print(f"     → Sample: {body['records'][0]}")
                else:
                    # Show errors
                    if isinstance(body, list) and body:
                        for error in body:
                            print(f"     → Error: {error.get('message', error)}")
                    elif isinstance(body, dict):
                        print(f"     → Error: {body.get('message', body)}")
                    else:
                        print(f"     → Error: {body}")
            
            return result
            
        else:
            print(f"  [ERROR] Status: {response.status_code}")
            print(f"  Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"  [ERROR] Request failed: {e}")
        return None

def demo_composite_api(sf):
    """Run demonstration of various Composite API operations"""
    
    print("\n" + "="*70)
    print("COMPOSITE API DEMONSTRATIONS")
    print("="*70)
    
    # Example 1: UPSERT Return + CREATE children
    print("\n\n[1] UPSERT Return__c and CREATE child ServiceReport__c records")
    input("Press Enter to continue...")
    request1 = build_composite_request_upsert_and_create()
    print(f"\nRequest JSON:")
    print(json.dumps(request1, indent=2))
    
    if input("\nExecute this request? (y/n): ").lower() == 'y':
        execute_composite_request(sf, request1, "UPSERT Return + CREATE children")
    
    # Example 2: UPDATE Return + READ children
    print("\n\n[2] UPDATE Return__c and READ ServiceReport__c records")
    input("Press Enter to continue...")
    request2 = build_composite_request_update_and_read()
    print(f"\nRequest JSON:")
    print(json.dumps(request2, indent=2))
    
    if input("\nExecute this request? (y/n): ").lower() == 'y':
        execute_composite_request(sf, request2, "UPDATE Return + READ children")
    
    # Example 3: UPSERT multiple Returns
    print("\n\n[3] UPSERT multiple Return__c records")
    input("Press Enter to continue...")
    request3 = build_composite_request_upsert_multiple_returns()
    print(f"\nRequest JSON (truncated):")
    print(json.dumps(request3, indent=2)[:500] + "...")
    
    if input("\nExecute this request? (y/n): ").lower() == 'y':
        execute_composite_request(sf, request3, "UPSERT multiple Returns")
    
    # Example 4: DELETE children + parent
    print("\n\n[4] DELETE ServiceReport__c records and Return__c")
    input("Press Enter to continue...")
    request4 = build_composite_request_delete()
    print(f"\nRequest JSON:")
    print(json.dumps(request4, indent=2))
    
    print("\n[WARNING] This will DELETE records!")
    if input("Execute this DELETE request? (y/n): ").lower() == 'y':
        execute_composite_request(sf, request4, "DELETE children + parent")
    
    # Example 5: Complete workflow
    print("\n\n[5] Complete workflow - UPSERT, CREATE, READ, UPDATE")
    input("Press Enter to continue...")
    request5 = build_composite_request_complete_workflow()
    print(f"\nRequest JSON (truncated):")
    print(json.dumps(request5, indent=2)[:500] + "...")
    
    if input("\nExecute this request? (y/n): ").lower() == 'y':
        execute_composite_request(sf, request5, "Complete workflow")

def main():
    """Main execution"""
    
    # Connect to Salesforce
    sf = connect_salesforce()
    
    print("\nComposite API allows you to:")
    print("  • Execute multiple operations in one HTTP request")
    print("  • Reference results from previous operations using @{referenceId.field}")
    print("  • Use External IDs for lookups and relationships")
    print("  • Reduce network latency and improve performance")
    print("  • Maintain transactional integrity with allOrNone=true")
    
    print("\n\nRelationship Patterns:")
    print("  Account.External_Id__c  → Employer__r: {External_Id__c: 'value'}")
    print("  Contact.External_Id__c  → Worker__r: {External_Id__c: 'value'}")
    print("  Return__c.External_Id__c → Return__c: '@{referenceId.id}'")
    print("  Case.External_Id__c     → Case__r: {External_Id__c: 'value'}")
    
    # Run demonstrations
    demo_composite_api(sf)
    
    print("\n\n" + "="*70)
    print("[COMPLETE] Composite API demonstration finished")
    print("="*70)

if __name__ == '__main__':
    main()
