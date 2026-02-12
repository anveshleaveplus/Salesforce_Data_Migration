# Salesforce Composite API - Return__c & ServiceReport__c

## Overview

Two Python scripts for managing Return__c and ServiceReport__c records using Salesforce Composite API:

1. **sit_return_composite_api.py** - Interactive demo with examples
2. **sit_return_composite_load.py** - Production loader from Oracle

## Benefits of Composite API

- **Performance**: Batch up to 25 operations in one HTTP request
- **Referential Integrity**: Create parent and children in single transaction
- **External ID Support**: Use External IDs for relationships (no need to query IDs first)
- **Reduced Network Latency**: Fewer round trips to Salesforce
- **Transaction Control**: `allOrNone` flag for all-or-nothing execution

## Data Model

```
Account (Employer)
  ↓ Employer__c (using External_Id__c)
Return__c
  ↓ Return__c (relationship)
ServiceReport__c (child)
  ↓ Worker__c (using External_Id__c)
Contact (Worker)
```

### External ID Relationships

| Salesforce Object | External ID Field | Oracle Source |
|-------------------|-------------------|---------------|
| Account | External_Id__c | EMPLOYER_ID |
| Contact | External_Id__c | WORKER_ID |
| Return__c | External_Id__c | WSR_ID |
| ServiceReport__c | External_Id__c | WSR_ID_SERVICE_ID |
| Case | External_Id__c | CLAIM_ID |

## Usage

### 1. Interactive Demo

```powershell
python sit\sit_return_composite_api.py
```

**Examples included:**
- UPSERT Return__c + CREATE children
- UPDATE Return__c + READ children
- UPSERT multiple Returns in batch
- DELETE children then parent
- Complete workflow (UPSERT → CREATE → READ → UPDATE)

### 2. Production Load from Oracle

```powershell
python sit\sit_return_composite_load.py
```

**Features:**
- Loads Return__c records from Oracle CO_WSR
- Creates child ServiceReport__c from CO_SERVICE
- Uses Composite API (5 Returns per request)
- Handles 25 operation limit per composite request
- Error tracking and reporting

## Composite API Patterns

### Pattern 1: UPSERT Parent with External ID

```json
{
  "method": "PATCH",
  "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_100001",
  "referenceId": "return1",
  "body": {
    "External_Id__c": "WSR_100001",
    "Employer__r": {"External_Id__c": "12345"},
    "ReturnSubmittedDate__c": "2026-01-15",
    "TotalDaysWorked__c": 120,
    "TotalWagesReported__c": 150000.00
  }
}
```

**Key Points:**
- Use `PATCH` for UPSERT operations
- URL includes External ID field name and value
- Lookup relationships use `__r` with nested External ID
- `referenceId` used to reference this record in subsequent operations

### Pattern 2: CREATE Child Referencing Parent

```json
{
  "method": "POST",
  "url": "/services/data/v59.0/sobjects/ServiceReport__c",
  "referenceId": "service1",
  "body": {
    "External_Id__c": "SR_100001_01",
    "Return__c": "@{return1.id}",
    "Worker__r": {"External_Id__c": "WORKER_9001"},
    "ServicePeriod__c": "202601",
    "DaysWorked__c": 30,
    "WagesEarned__c": 7500.00
  }
}
```

**Key Points:**
- Use `POST` for CREATE operations
- `Return__c: "@{return1.id}"` references parent from previous operation
- `@{referenceId.field}` syntax accesses previous response values
- Worker lookup uses External ID

### Pattern 3: UPDATE Existing Record

```json
{
  "method": "PATCH",
  "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_100001",
  "referenceId": "returnUpdate",
  "body": {
    "TotalDaysWorked__c": 150,
    "Charges__c": 4375.00,
    "Interest__c": 125.50
  }
}
```

**Key Points:**
- Use `PATCH` with External ID for UPDATE
- Only include fields to update
- Omit External_Id__c in body when it's in URL

### Pattern 4: READ (Query) Records

```json
{
  "method": "GET",
  "url": "/services/data/v59.0/query?q=SELECT+Id,External_Id__c,DaysWorked__c+FROM+ServiceReport__c+WHERE+Return__r.External_Id__c='WSR_100001'",
  "referenceId": "serviceReportsRead"
}
```

**Key Points:**
- Use `GET` with SOQL query
- URL-encode spaces as `+`
- Can filter by parent's External ID using `Return__r.External_Id__c`

### Pattern 5: DELETE Records

```json
{
  "method": "DELETE",
  "url": "/services/data/v59.0/sobjects/ServiceReport__c/External_Id__c/SR_100001_01",
  "referenceId": "deleteService1"
}
```

**Key Points:**
- Use `DELETE` method
- Must delete children before parent (referential integrity)
- Consider `allOrNone: true` for delete operations
- Returns HTTP 204 on success

## Complete Composite Request Structure

```json
{
  "allOrNone": false,
  "compositeRequest": [
    {
      "method": "PATCH",
      "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_100001",
      "referenceId": "return1",
      "body": { ... }
    },
    {
      "method": "POST",
      "url": "/services/data/v59.0/sobjects/ServiceReport__c",
      "referenceId": "service1",
      "body": {
        "Return__c": "@{return1.id}",
        ...
      }
    }
  ]
}
```

## Field Mappings

### Return__c Fields

| API Name | Type | Oracle Source | Description |
|----------|------|---------------|-------------|
| External_Id__c | Text | CO_WSR.WSR_ID | Unique return identifier |
| Employer__c | Lookup(Account) | CO_WSR.EMPLOYER_ID | Employer reference |
| ReturnSubmittedDate__c | Date | CO_WSR.DATE_RECEIVED | Submission date |
| TotalDaysWorked__c | Number(18,0) | CO_WSR.EMPLOYER_DAYS | Total days |
| TotalWagesReported__c | Currency(16,2) | CO_WSR.EMPLOYER_TOTAL_WAGES | Total wages |
| Charges__c | Currency(16,2) | CO_WSR_TOTALS.CONTRIBUTION_AMOUNT | Contribution charges |
| Interest__c | Currency(12,2) | CO_ADJUSTMENT.STATUATORY_INTEREST | Interest amount |
| InvoiceAmount__c | Currency(12,2) | CO_ACC_INVOICE_DETAIL.AMOUNT | Invoice amount |
| InvoiceDueDate__c | Date | CO_ACC_INVOICE.PAYMENT_DUE_DATE | Payment due date |
| ReturnType__c | Picklist | CO_WSR.EVENT_TYPE_CODE | Return type |
| InvoiceStatus__c | Picklist | CO_ACC_INVOICE_DETAIL.STATUS_CODE | Invoice status |

### ServiceReport__c Fields

| API Name | Type | Oracle Source | Description |
|----------|------|---------------|-------------|
| External_Id__c | Text | WSR_ID + SERVICE_ID | Unique service identifier |
| Return__c | Lookup(Return__c) | CO_SERVICE.WSR_ID | Parent return |
| Worker__c | Lookup(Contact) | CO_SERVICE.WORKER | Worker reference |
| ServicePeriod__c | Text(10) | CO_SERVICE.PERIOD_END | Period (YYYYMM) |
| DaysWorked__c | Number(18,0) | CO_SERVICE.DAYS_WORKED | Days worked |
| WagesEarned__c | Currency(18,2) | CO_SERVICE.WAGES | Wages amount |

## HTTP Status Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 200 | OK | Successful GET or PATCH |
| 201 | Created | Successful POST (new record) |
| 204 | No Content | Successful DELETE |
| 400 | Bad Request | Invalid request format |
| 404 | Not Found | Record not found |

## Limits and Considerations

1. **25 Sub-requests Maximum** per Composite request
2. **5 levels maximum** of reference depth (`@{ref1.@{ref2.id}}` not allowed)
3. **All or None**: Set `allOrNone: true` for transactional behavior
4. **API Limits**: Composite requests count as 1 API call + 1 per sub-request
5. **Timeout**: Default 120 seconds

## Batching Strategy

For optimal performance with Return__c + ServiceReport__c:

```
Average 4 service reports per return:
- 1 UPSERT Return__c
- 4 POST ServiceReport__c
= 5 operations per return

Max 25 operations per composite = ~5 returns per composite request
```

**Configuration:**
```python
COMPOSITE_BATCH_SIZE = 5  # Returns per request
# Results in ~5 returns × 5 operations = 25 operations
```

## Error Handling

Composite API returns success/failure for each sub-request:

```json
{
  "compositeResponse": [
    {
      "referenceId": "return1",
      "httpStatusCode": 201,
      "body": {
        "id": "a0X5g000001AbCdEAK",
        "success": true,
        "created": true
      }
    },
    {
      "referenceId": "service1",
      "httpStatusCode": 400,
      "body": [
        {
          "message": "Required field missing: Worker__c",
          "errorCode": "REQUIRED_FIELD_MISSING"
        }
      ]
    }
  ]
}
```

**Error handling in code:**
```python
for sub_response in result['compositeResponse']:
    if sub_response['httpStatusCode'] in [200, 201, 204]:
        success_count += 1
    else:
        error_count += 1
        errors.append({
            'ref': sub_response['referenceId'],
            'error': sub_response['body']
        })
```

## Prerequisites

### Salesforce Configuration

1. **External ID Fields** must be created and marked as External ID:
   - Account.External_Id__c
   - Contact.External_Id__c
   - Return__c.External_Id__c
   - ServiceReport__c.External_Id__c

2. **Field-Level Security**: Ensure API user has read/write access to all fields

3. **Object Permissions**: API user needs Create, Read, Update, Delete on Return__c and ServiceReport__c

### Python Dependencies

```bash
pip install simple-salesforce oracledb pandas python-dotenv requests
```

### Environment Variables (.env.sit)

```ini
# Oracle
ORACLE_USER=your_user
ORACLE_PASSWORD=your_password
ORACLE_HOST=your_host
ORACLE_PORT=1521
ORACLE_SID=your_sid

# Salesforce SIT
SF_USERNAME=your_sf_user@example.com.sit
SF_PASSWORD=your_sf_password
SF_SECURITY_TOKEN=your_token
SF_DOMAIN=test
```

## Performance Comparison

### Traditional Bulk API
```
1,000 Returns with 4,000 Service Reports:
- 1 Bulk API call for Returns: ~5 seconds
- 1 Bulk API call for Services: ~8 seconds
- Wait for async processing: ~30 seconds
Total: ~43 seconds
```

### Composite API
```
1,000 Returns with 4,000 Service Reports:
- 200 Composite requests (5 returns each): ~40 seconds
- No async wait needed
- Real-time error feedback
Total: ~40 seconds (+ immediate feedback)
```

**Benefits:**
- Similar performance for bulk loads
- Better for smaller batches (<1000 records)
- Immediate error feedback
- Maintains parent-child integrity in single request

## Advanced Patterns

### Pattern: Conditional Operations

Use query results to conditionally create/update:

```json
{
  "compositeRequest": [
    {
      "method": "GET",
      "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_100001",
      "referenceId": "checkReturn"
    },
    {
      "method": "POST",
      "url": "/services/data/v59.0/sobjects/ServiceReport__c",
      "referenceId": "addService",
      "body": {
        "Return__c": "@{checkReturn.id}",
        ...
      }
    }
  ]
}
```

### Pattern: Aggregate Then Update

Query aggregates and update parent:

```json
{
  "compositeRequest": [
    {
      "method": "GET",
      "url": "/services/data/v59.0/query?q=SELECT+SUM(DaysWorked__c)+total+FROM+ServiceReport__c+WHERE+Return__r.External_Id__c='WSR_100001'",
      "referenceId": "calcTotal"
    },
    {
      "method": "PATCH",
      "url": "/services/data/v59.0/sobjects/Return__c/External_Id__c/WSR_100001",
      "referenceId": "updateReturn",
      "body": {
        "TotalDaysWorked__c": "@{calcTotal.records[0].total}"
      }
    }
  ]
}
```

## Troubleshooting

### Issue: "External ID not found"
**Solution**: Ensure External_Id__c field exists and is marked as External ID checkbox

### Issue: "INVALID_CROSS_REFERENCE_KEY"
**Solution**: Referenced External ID value doesn't exist (e.g., Employer not found)

### Issue: "CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY"
**Solution**: Check triggers/validation rules, may need to bypass for data loads

### Issue: "Reference not found: @{return1.id}"
**Solution**: Ensure referenceId matches exactly (case-sensitive)

### Issue: "Exceeded max sub-requests (25)"
**Solution**: Reduce batch size or split into multiple composite requests

## See Also

- [Salesforce Composite API Documentation](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_composite_composite.htm)
- [External ID Best Practices](https://help.salesforce.com/s/articleView?id=sf.fields_about_external_id.htm)
- sit_account_load.py - Account loading script
- sit_contact_load.py - Contact loading script
