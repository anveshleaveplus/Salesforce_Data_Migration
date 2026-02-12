# Proposed Salesforce Fields for Return__c and Claim__c

## Return__c (Employer Returns)
**Oracle Source:** CO_BACK_RETURN_REQUEST (6,531 records)

### Core Fields

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| EMPLOYER_ID | Account__c | Lookup(Account) | - | Yes | Link to employer account |
| BACK_RETURN_REQUEST_ID | External_ID__c | Text | 20 | Yes | Oracle Return ID (unique, external ID) |
| BACK_RETURN_STATUS_CODE | Status__c | Picklist | - | No | Return status code |
| RETURN_TYPE_CODE | Return_Type__c | Picklist | - | No | Type of return |
| REQUESTED_DATE | Requested_Date__c | Date | - | No | When return was requested |
| REQUESTED_BY | Requested_By__c | Text | 30 | No | User who requested |
| PROCESSED_DATE | Processed_Date__c | Date | - | No | When return was processed |
| BACK_RETURN_REQUEST_ID¹ | Back_Return_Request_ID__c | Number | 18,0 | No | Period reference ID |

**Name field:** Auto-number (e.g., RET-000001) ✓ Already exists

**Relationships:**
- Account__c → Account (Master-Detail or Lookup)

**Notes:**
- Need picklist values for Status__c from CO_CODE table
- Need picklist values for Return_Type__c from CO_CODE table
- May also need fields from CO_BACK_RETURN_PERIOD (period dates)

---

## Claim__c (Worker Benefits/Claims)
**Oracle Source:** CO_CLAIM (336,341 records)

### Core Identity Fields

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| WORKER_ID | Contact__c | Lookup(Contact) | - | Yes | Link to worker contact |
| CLAIM_ID | External_ID__c | Text | 20 | Yes | Oracle Claim ID (unique, external ID) |
| CLAIM_SUB_ID | Claim_Sub_ID__c | Number | 18,0 | No | Sub-claim identifier |

### Dates & Leave Details

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| LEAVE_FROM_DATE | Leave_From_Date__c | Date | - | No | Start date of leave |
| PAYMENT_REQUIRED_DATE | Payment_Required_Date__c | Date | - | No | When payment is due |
| CLAIM_PAYMENT_DATE | Claim_Payment_Date__c | Date | - | No | Actual payment date |
| TERMINATION_DATE | Termination_Date__c | Date | - | No | Employment termination date |
| CLAIM_RECEIVED_DATE | Claim_Received_Date__c | Date | - | No | When claim was received |
| TOTAL_NO_OF_WEEKS | Total_Number_of_Weeks__c | Number | 5,2 | No | Total weeks claimed |
| SERVICE_PERIOD | Service_Period__c | Text | 10 | No | Service period (YYYYMM format) |

### Status & Type

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| CLAIM_STATUS_CODE | Claim_Status__c | Picklist | - | No | Current claim status |
| CLAIM_TYPE_CODE | Claim_Type__c | Picklist | - | No | Type of claim |
| PAYMENT_TYPE_CODE | Payment_Type__c | Picklist | - | No | Payment method type |

### Financial - Employer Contribution

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| EMPLOYER_ID | Account__c | Lookup(Account) | - | No | Current/claiming employer |
| EMPLOYER_NO_OF_WEEKS | Employer_Number_of_Weeks__c | Number | 5,2 | No | Weeks from this employer |
| EMPLOYER_WEEKLY_WAGE_TOTAL | Employer_Weekly_Wage_Total__c | Currency | 18,2 | No | Total weekly wage |
| EMPLOYER_WEEKLY_ALLOWANCES | Employer_Weekly_Allowances__c | Currency | 18,2 | No | Weekly allowances |
| EMPLOYER_WEEKLY_NET_AMOUNT | Employer_Weekly_Net_Amount__c | Currency | 18,2 | No | Net weekly amount |

### Financial - Tax & Wages

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| TAX_FILE_NUMBER | Tax_File_Number__c | Text (Encrypted) | 24 | No | Worker's TFN (encrypted) |
| TAX_SCALE_CODE | Tax_Scale__c | Picklist | - | No | Tax scale code |
| ANNUAL_TAX_REBATE | Annual_Tax_Rebate__c | Currency | 18,2 | No | Annual rebate amount |
| SUGGESTED_WAGE | Suggested_Wage__c | Currency | 18,2 | No | System suggested wage |
| EMPLOYER_PROVIDED_WAGE | Employer_Provided_Wage__c | Currency | 18,2 | No | Wage provided by employer |
| CALCULATED_AVERAGE_WAGE | Calculated_Average_Wage__c | Currency | 18,2 | No | Calculated average |
| CALCULATED_ROP_WAGE | Calculated_ROP_Wage__c | Currency | 18,2 | No | Rate of pay wage |

### Payment Details

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| BANK_ACCOUNT_NAME | Bank_Account_Name__c | Text (Encrypted) | 120 | No | Account holder name |
| BSB_NUMBER | BSB_Number__c | Text (Encrypted) | 44 | No | Bank BSB number |
| BANK_ACCOUNT_NUMBER | Bank_Account_Number__c | Text (Encrypted) | 64 | No | Bank account number |
| BANK_ID | Bank_ID__c | Text | 5 | No | Bank identifier |

### WSC (Worker Service Continuity)

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| WSC_AMOUNT | WSC_Amount__c | Currency | 18,2 | No | WSC payment amount |
| WSC_INTEREST_AMOUNT | WSC_Interest_Amount__c | Currency | 18,2 | No | WSC interest amount |
| REQUESTED_WSC_AMOUNT | Requested_WSC_Amount__c | Currency | 18,2 | No | Requested WSC amount |
| QUARANTINED_WSC_WEEKS | Quarantined_WSC_Weeks__c | Number | 5,2 | No | Quarantined WSC weeks |
| QUARANTINED_VIC_WEEKS | Quarantined_VIC_Weeks__c | Number | 5,2 | No | Quarantined VIC weeks |

### Extra Service & Adjustments

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| EXTRA_SERVICE_DUE | Extra_Service_Due__c | Number | 5,1 | No | Extra service due |
| EXTRA_SERVICE_PERIOD | Extra_Service_Period__c | Number | 18,0 | No | Extra service period |
| ADJUSTMENT_ID | Adjustment_ID__c | Number | 18,0 | No | Related adjustment |

### Worker Details

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| WORKER_POSITION | Worker_Position__c | Picklist | - | No | Worker position code |
| EMPLOYMENT_TYPE | Employment_Type__c | Picklist | - | No | Employment type code |
| WORKER_TYPE_CODE | Worker_Type__c | Picklist | - | No | Worker classification |
| PAY_CLASSIFICATION_OF_WORKER | Pay_Classification__c | Text | 200 | No | Pay classification |
| AWARD | Award__c | Text | 35 | No | Award/agreement |

### Approvals & Processing

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| PROCESSED_BY | Processed_By__c | Text | 30 | No | Who processed claim |
| PROCESSED_DATE | Processed_Date__c | DateTime | - | No | When processed |
| APPROVED_BY | Approved_By__c | Text | 30 | No | Who approved |
| APPROVED_DATE | Approved_Date__c | DateTime | - | No | When approved |
| MGR_APPROVED_BY | Manager_Approved_By__c | Text | 30 | No | Manager who approved |
| MGR_APPROVED_DATE | Manager_Approved_Date__c | DateTime | - | No | Manager approval date |

### Flags & Indicators (Yes/No fields)

| Oracle Column | SF Field Name | Type | Description |
|--------------|---------------|------|-------------|
| IS_APPROVED_BY_EMPLOYER | Is_Approved_By_Employer__c | Checkbox | Employer approved |
| IS_ELIGIBLE_TO_CLAIM_TFT | Is_Eligible_Tax_Free_Threshold__c | Checkbox | Tax free threshold eligible |
| IS_DEPENDANT_CHILDREN | Has_Dependent_Children__c | Checkbox | Has dependent children |
| IS_DEPENDANT_SPOUSE | Has_Dependent_Spouse__c | Checkbox | Has dependent spouse |
| IS_SOLE_PARENT | Is_Sole_Parent__c | Checkbox | Sole parent |
| IS_WORKER_SIGNATURE | Worker_Signed__c | Checkbox | Worker signed form |
| IS_CHECK_PERSONAL_DETAIL | Check_Personal_Details__c | Checkbox | Personal details verified |
| IS_INCLUDE_WSC_INTEREST_PAY | Include_WSC_Interest__c | Checkbox | Include WSC interest |
| IS_TO_PAY_WSC_FIRST | Pay_WSC_First__c | Checkbox | Pay WSC first |
| IS_INTERSTATE_SERVICE | Has_Interstate_Service__c | Checkbox | Interstate service |
| IS_ALL_LEAVE | Is_All_Leave__c | Checkbox | Claiming all leave |
| IS_LSL_PAID_BY_EMPLOYER | LSL_Paid_By_Employer__c | Checkbox | LSL paid by employer |
| IS_SUGGESTED_WAGE_INDEXED | Suggested_Wage_Indexed__c | Checkbox | Wage indexed |
| IS_WORKER_ACCEPT_WAGE | Worker_Accepted_Wage__c | Checkbox | Worker accepted wage |
| IS_SUGGESTED_WAGE_OVERRIDDEN | Suggested_Wage_Overridden__c | Checkbox | Wage overridden |
| IS_PROCESS_WAGE_INDEXED | Process_Wage_Indexed__c | Checkbox | Process wage indexed |
| IS_MAKE_UP_PAYMENT_REQUIRED | Makeup_Payment_Required__c | Checkbox | Makeup payment required |

### Wage Verification

| Oracle Column | SF Field Name | Type | Length | Description |
|--------------|---------------|------|--------|-------------|
| NOT_SENDING_WAGEVERF_REASON | Not_Sending_Wage_Verification_Reason__c | Text | 100 | Why not sending wage verification |
| WAGEVERF_LAST_SEND_DATE | Wage_Verification_Last_Sent__c | DateTime | - | Last wage verification sent |
| WAGEVERF_SEND_COUNT | Wage_Verification_Send_Count__c | Number | 18,0 | Times wage verification sent |
| WAGEVERF_SUBMIT_DATE | Wage_Verification_Submitted__c | DateTime | - | Wage verification submitted |
| WAGE_VERIFICATION_SENT_BY | Wage_Verification_Sent_By__c | Text | 30 | Who sent wage verification |
| IS_FAILED_TO_CONTACT_EMPLOYER | Failed_To_Contact_Employer__c | Checkbox | - | Failed to contact employer |
| IS_WAGE_PROVIDED_BY_WAGEVERF | Wage_By_Verification__c | Checkbox | - | Wage from verification |
| IS_WAGE_PROVIDED_BY_PAYSLIP | Wage_By_Payslip__c | Checkbox | - | Wage from payslip |
| PAYSLIP_RECV_DATE | Payslip_Received_Date__c | DateTime | - | Payslip received date |
| WAGE_VERF_INITIATED_WHEN | Wage_Verification_Initiated__c | DateTime | - | When initiated |
| WAGE_VERF_INITIATED_BY | Wage_Verification_Initiated_By__c | Text | 30 | Who initiated |
| WAGE_INDEX_PERIOD | Wage_Index_Period__c | Number | 18,0 | Wage index period |
| INDEXED_AMOUNT | Indexed_Amount__c | Currency | 18,2 | Indexed amount |

### Make-up Payments

| Oracle Column | SF Field Name | Type | Length | Description |
|--------------|---------------|------|--------|-------------|
| MAKE_UP_PAYMENT_STATUS | Makeup_Payment_Status__c | Picklist | - | Status of makeup payment |
| MAKE_UP_PAYMENT_PROCESSED_DATE | Makeup_Payment_Processed_Date__c | Date | - | When processed |
| IS_WSC_INTEREST_ONLY_CLAIM | WSC_Interest_Only_Claim__c | Checkbox | - | WSC interest only |

### Return to MSO

| Oracle Column | SF Field Name | Type | Length | Description |
|--------------|---------------|------|--------|-------------|
| RETURN_TO_MSO_REASON_CODE | Return_To_MSO_Reason__c | Picklist | - | Reason for return |
| RETURN_TO_MSO_COMMENT | Return_To_MSO_Comment__c | Text Area | 500 | Return comment |

### Email & Communication

| Oracle Column | SF Field Name | Type | Length | Description |
|--------------|---------------|------|--------|-------------|
| EMAIL_STATUS_CODE | Email_Status__c | Picklist | - | Email status |
| EMAIL_SENT_DATE | Email_Sent_Date__c | DateTime | - | Email sent date |

### Notes & Miscellaneous

| Oracle Column | SF Field Name | Type | Length | Description |
|--------------|---------------|------|--------|-------------|
| REMARKS | Remarks__c | Long Text Area | 32,000 | General remarks (Oracle: 740) |
| AVERAGE_CALCULATOR_NOTE | Average_Calculator_Note__c | Long Text Area | 32,000 | Calculator note 1 (Oracle: 2000) |
| AVERAGE_CALCULATOR_NOTE_2 | Average_Calculator_Note_2__c | Long Text Area | 32,000 | Calculator note 2 (Oracle: 2000) |
| EXECUTOR_ESTATE_NAME | Executor_Estate_Name__c | Text | 60 | Estate executor name |
| CLAIM_ELIGIBLE_REASON_CODE | Claim_Eligible_Reason__c | Picklist | - | Eligibility reason |
| CLAIM_PAYMENT_TAX_YEAR_CODE | Claim_Payment_Tax_Year__c | Text | 5 | Tax year code |
| PROMOTION_GIFT_CODE | Promotion_Gift__c | Picklist | - | Promotional gift |
| PRIMARY_DOCUMENT_ID | Primary_Document_ID__c | Text | 38 | Document GUID |
| ALTERNATIVE_ADDRESS_ID | Alternative_Address_ID__c | Number | 18,0 | Alt address reference |
| IS_TO_CHECK_PAY_CLASSIFICATION | Check_Pay_Classification__c | Checkbox | - | Check pay classification |

### Override Flags

| Oracle Column | SF Field Name | Type | Description |
|--------------|---------------|------|-------------|
| IS_OVERRIDE_BSB | Override_BSB__c | Checkbox | BSB overridden |
| IS_OVERRIDE_AC_NAME | Override_Account_Name__c | Checkbox | Account name overridden |
| IS_OVERRIDE_AC_NUMBER | Override_Account_Number__c | Checkbox | Account number overridden |
| IS_OVERRIDE_TFN | Override_TFN__c | Checkbox | TFN overridden |
| IS_OVERRIDE_WAGE | Override_Wage__c | Checkbox | Wage overridden |
| IS_OVERRIDE_ALTERNATE_ADDRESS | Override_Alternate_Address__c | Checkbox | Alt address overridden |

### System Fields

| Oracle Column | SF Field Name | Type | Length | Description |
|--------------|---------------|------|--------|-------------|
| CREATED_BY | Oracle_Created_By__c | Text | 30 | Oracle user who created |
| CREATED_WHEN | Oracle_Created_Date__c | DateTime | - | Oracle creation date |
| MODIFIED_BY | Oracle_Modified_By__c | Text | 30 | Oracle user who modified |
| MODIFIED_WHEN | Oracle_Modified_Date__c | DateTime | - | Oracle modification date |
| UPDATE_COUNTER | Oracle_Update_Counter__c | Number | 18,0 | Oracle update count |
| ORACLE_TRANSACTION_ID | Oracle_Transaction_ID__c | Text | 128 | Oracle transaction ID |
| AUDIT_TRAIL_ID | Oracle_Audit_Trail_ID__c | Number | 18,0 | Oracle audit trail ID |
| ENCRYPTED_FIELDS | Has_Encrypted_Fields__c | Checkbox | - | Has encrypted data |

**Name field:** Auto-number (e.g., C-00001) ✓ Already exists

---

## ClaimComponent__c (Claim Payment Breakdown)
**Oracle Source:** CO_CLAIM_PAYMENT (368,976 records)

### Fields Already Present
- Claim__c (Lookup to Claim__c) ✓ Already exists

### Additional Fields Needed

| Oracle Column | SF Field Name | Type | Length | Required | Description |
|--------------|---------------|------|--------|----------|-------------|
| WORKER_TRANSACTION_ID | Worker_Transaction_ID__c | Text | 20 | Yes | External ID (unique) |
| NO_OF_WEEKS | Number_of_Weeks__c | Number | 5,2 | No | Total weeks |
| WEEKLY_RATE | Weekly_Rate__c | Currency | 18,2 | No | Weekly payment rate |
| WEEKLY_GROSS_RATE | Weekly_Gross_Rate__c | Currency | 18,2 | No | Weekly gross rate |
| WEEKLY_ALLOWANCES | Weekly_Allowances__c | Currency | 18,2 | No | Weekly allowances |
| TAX_AMOUNT | Tax_Amount__c | Currency | 18,2 | No | Total tax amount |

### Tax Component Breakdown

| Oracle Column | SF Field Name | Type | Length | Description |
|--------------|---------------|------|--------|-------------|
| PRE78_COMPONENT_WEEKS | Pre_1978_Weeks__c | Number | 8,2 | Weeks pre-1978 |
| PRE78_TAX_AMOUNT | Pre_1978_Tax__c | Currency | 18,2 | Tax on pre-1978 |
| PRE93_COMPONENT_WEEKS | Pre_1993_Weeks__c | Number | 5,2 | Weeks 1978-1993 |
| PRE93_TAX_AMOUNT | Pre_1993_Tax__c | Currency | 18,2 | Tax on 1978-1993 |
| POST93_COMPONENT_WEEKS | Post_1993_Weeks__c | Number | 5,2 | Weeks post-1993 |
| POST93_TAX_AMOUNT | Post_1993_Tax__c | Currency | 18,2 | Tax on post-1993 |

### System Fields

| Oracle Column | SF Field Name | Type | Length | Description |
|--------------|---------------|------|--------|-------------|
| PAYMENT_TYPE_CODE | Payment_Type__c | Picklist | - | Payment type |
| CREATED_BY | Oracle_Created_By__c | Text | 30 | Oracle creator |
| CREATED_WHEN | Oracle_Created_Date__c | DateTime | - | Oracle created date |
| MODIFIED_BY | Oracle_Modified_By__c | Text | 30 | Oracle modifier |
| MODIFIED_WHEN | Oracle_Modified_Date__c | DateTime | - | Oracle modified date |
| UPDATE_COUNTER | Oracle_Update_Counter__c | Number | 18,0 | Update counter |
| AUDIT_TRAIL_ID | Oracle_Audit_Trail_ID__c | Number | 18,0 | Audit trail ID |
| ORACLE_TRANSACTION_ID | Oracle_Transaction_ID__c | Text | 128 | Transaction ID |

---

## Notes & Recommendations

### Security & Encryption
- **TFN, BSB, Bank Account fields** marked as encrypted in Oracle
- Recommend using **Salesforce Shield** or **Custom Encryption** for these fields
- Alternative: Store encrypted values as-is and decrypt in application layer

### Picklist Values
Need to query **CO_CODE** table for these picklists:
- CLAIM_STATUS_CODE → Claim_Status__c values
- CLAIM_TYPE_CODE → Claim_Type__c values
- PAYMENT_TYPE_CODE → Payment_Type__c values
- BACK_RETURN_STATUS_CODE → Status__c values (Return)
- RETURN_TYPE_CODE → Return_Type__c values
- TAX_SCALE_CODE → Tax_Scale__c values
- WORKER_POSITION → Worker_Position__c values
- EMPLOYMENT_TYPE → Employment_Type__c values
- WORKER_TYPE_CODE → Worker_Type__c values
- And others marked as "Code" fields

### Field Priority (Phase 1 - Essential)
**Return__c:**
- Account__c, External_ID__c, Status__c, Requested_Date__c, Processed_Date__c

**Claim__c:**
- Contact__c, External_ID__c, Claim_Status__c, Claim_Type__c
- Leave_From_Date__c, Total_Number_of_Weeks__c, Claim_Payment_Date__c
- Employer_Weekly_Wage_Total__c, Account__c (employer)

**ClaimComponent__c:**
- Worker_Transaction_ID__c, Number_of_Weeks__c, Weekly_Rate__c

### Field Priority (Phase 2 - Important)
All financial fields, dates, approvals, WSC fields

### Field Priority (Phase 3 - Optional)
Flags, overrides, wage verification tracking, notes

### Volume Considerations
- **CO_CLAIM:** 336,341 records
- **CO_CLAIM_PAYMENT:** 368,976 records  
- **CO_BACK_RETURN_REQUEST:** 6,531 records

Plan for batch loading with **bulk API** and proper **External ID** matching.

### Relationships
- **Claim__c.Contact__c** → Contact (Worker) - **Master-Detail** recommended
- **Claim__c.Account__c** → Account (Employer) - **Lookup** (optional, can be derived from Contact's AccountContactRelation)
- **ClaimComponent__c.Claim__c** → Claim__c - **Master-Detail** ✓ Already configured
- **Return__c.Account__c** → Account (Employer) - **Master-Detail** recommended
