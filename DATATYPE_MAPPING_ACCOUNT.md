# DATA TYPE MAPPING - ACCOUNT OBJECT

## Source Systems

### 1. Oracle Database (SCH_CO_20)
| Table | Column | Data Type | Max Length |
|-------|--------|-----------|------------|
| CO_EMPLOYER | CUSTOMER_ID | NUMBER(7) | 7 digits |
| CO_EMPLOYER | ABN | NUMBER(11) | 11 digits |
| CO_EMPLOYER | ACN | NUMBER(9) | 9 digits |
| CO_EMPLOYER | TRADING_NAME | VARCHAR2(150) | 150 chars |
| CO_WSR_SERVICE | EMPLOYMENT_START_DATE | TIMESTAMP(6) | Date/Time |

### 2. SQL Server (cosql-test.coinvest.com.au / AvatarWarehouse.datascience.abr_cleaned)
| Column | Data Type | Notes |
|--------|-----------|-------|
| Australian Business Number | bigint | Join key with Oracle ABN |
| ABN Registration - Date of Effect | date | Registration date |
| ABN Status | varchar(MAX) | Picklist values: "Active", "Cancelled" |
| Main - Industry Class | varchar(MAX) | Industry classification (e.g., "House Construction") |
| Main - Industry Class Code | bigint | Industry code number (e.g., 3011, 3242) |

## Destination: Salesforce Account Object
| Field | Type | Length | Required | External ID | Notes |
|-------|------|--------|----------|-------------|-------|
| External_Id__c | string | 50 | No | ✅ Yes | Oracle CUSTOMER_ID |
| Registration_Number__c | string | 50 | No | No | Duplicate of External_Id__c |
| ABN__c | string | 12 | No | No | Oracle ABN (11 digits) |
| ACN__c | string | 12 | No | No | Oracle ACN (9 digits) |
| Name | string | 255 | ✅ **YES** | No | Oracle TRADING_NAME |
| RegisteredEntityName__c | string | 255 | No | No | Oracle TRADING_NAME (duplicate) |
| TradingAs__c | string | 255 | No | No | Oracle TRADING_NAME (duplicate) |
| DateEmploymentCommenced__c | date | - | No | No | Oracle EMPLOYMENT_START_DATE |
| ABNRegistrationDate__c | date | - | No | No | SQL: ABN Registration Date |
| AccountStatus__c | picklist | 255 | No | No | SQL: ABN Status |
| Classifications__c | picklist | 255 | No | No | SQL: Industry Class |
| OSCACode__c | string | 50 | No | No | SQL: Industry Class Code |

## Data Type Compatibility Matrix

### ✅ Compatible Mappings
| Source | Type | Destination | Type | Status |
|--------|------|-------------|------|--------|
| CO_EMPLOYER.CUSTOMER_ID | NUMBER(7) | External_Id__c | string(50) | ✅ OK |
| CO_EMPLOYER.ABN | NUMBER(11) | ABN__c | string(12) | ✅ OK |
| CO_EMPLOYER.ACN | NUMBER(9) | ACN__c | string(12) | ✅ OK |
| CO_EMPLOYER.TRADING_NAME | VARCHAR2(150) | Name | string(255) | ✅ OK |
| CO_EMPLOYER.TRADING_NAME | VARCHAR2(150) | RegisteredEntityName__c | string(255) | ✅ OK |
| CO_EMPLOYER.TRADING_NAME | VARCHAR2(150) | TradingAs__c | string(255) | ✅ OK |
| CO_WSR_SERVICE.EMPLOYMENT_START_DATE | TIMESTAMP | DateEmploymentCommenced__c | date | ✅ OK |
| SQL: ABN Registration Date | date | ABNRegistrationDate__c | date | ✅ OK |

### ⚠️ Requires Validation
| Source | Type | Destination | Type | Issue |
|--------|------|-------------|------|-------|
| SQL: ABN Status | varchar(MAX) | AccountStatus__c | picklist(255) | Need to verify all values exist in picklist |
| SQL: Industry Class | varchar(MAX) | Classifications__c | picklist(255) | Need to verify all values exist in picklist |
| SQL: Industry Class Code | bigint | OSCACode__c | string(50) | Need to convert bigint to string |

## Sample Data from SQL Server

### ABN Status Values (Distinct)
- Active
- Cancelled

### Industry Class Examples
- House Construction
- Carpentry Services
- Plumbing Services
- Roofing Services
- Bricklaying Services
- Electrical Services
- Other Construction Services n.e.c.
- Land Development and Subdivision
- Non-Residential Building Construction

### Industry Class Codes (Sample)
- 3011 (House Construction)
- 3242 (Carpentry Services)
- 3231 (Plumbing Services)
- 3223 (Roofing Services)
- 3222 (Bricklaying Services)
- 3232 (Electrical Services)
- 3299 (Other Construction Services)

## Transformation Notes

1. **Number to String Conversion**
   - Oracle: NUMBER fields converted to string, remove .0 suffix
   - SQL: bigint Industry Code converted to string

2. **Date Conversion**
   - Oracle TIMESTAMP → ISO format string 'YYYY-MM-DD'
   - SQL date → ISO format string 'YYYY-MM-DD'

3. **Join Logic**
   - Oracle ABN (NUMBER) converted to string without spaces
   - SQL Australian Business Number (bigint) converted to string
   - LEFT JOIN on cleaned ABN values

4. **Picklist Values**
   - AccountStatus__c: Must contain "Active" and "Cancelled"
   - Classifications__c: Must contain all ANZSIC industry class values
   - Values must be pre-created in Salesforce before data load

## Recommendations

1. **Before Loading Data:**
   - ✅ Verify External_Id__c is marked as External ID field
   - ✅ Ensure Name field is populated (required field)
   - ⚠️ Create picklist values for AccountStatus__c ("Active", "Cancelled")
   - ⚠️ Create picklist values for Classifications__c (all ANZSIC codes)
   - ⚠️ Verify OSCACode__c can hold industry codes (up to 4-5 digits)

2. **Field Length Validation:**
   - TRADING_NAME (150) fits in Name (255) ✅
   - ABN (11 digits) fits in ABN__c (12 chars) ✅
   - ACN (9 digits) fits in ACN__c (12 chars) ✅
   - Industry Code (bigint) fits in OSCACode__c (50 chars) ✅

3. **Data Quality Checks:**
   - Handle NULL values for optional SQL Server fields
   - Validate ABN format before joining
   - Check for unmatched ABNs between Oracle and SQL Server
