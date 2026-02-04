# SIT Contact Load - Missing External ID Field

**Date:** February 3, 2026  
**Environment:** SIT (dataadmin@leaveplus.com.au.sit)  
**Issue:** Cannot load Contacts - Missing External_Id__c field

---

## Problem

The Contact object in the SIT environment is missing the **External_Id__c** custom field that is required for loading Oracle data.

### Error Message
```
ERROR at Row:1:Column:12
No such column 'External_Id__c' on entity 'Contact'. 
If you are attempting to use a custom field, be sure to append the '__c' 
after the custom field name.
```

### Current State
- ✅ Account object has External_Id__c field (working correctly)
- ❌ Contact object is missing External_Id__c field
- Total Contacts in SIT: 3 (only test records)

---

## Required Action

**Please add the External_Id__c field to the Contact object in SIT:**

### Field Specifications
- **Field Label:** External Id
- **API Name:** External_Id__c
- **Data Type:** Text(18) or Number(18,0)
- **Required:** No (unchecked)
- **Unique:** Yes (checked)
- **External ID:** Yes (checked)
- **Case Sensitive:** No (unchecked)

### Field Purpose
This field stores the Oracle CO_WORKER.CUSTOMER_ID (Worker ID) and serves as:
1. **Unique identifier** for each contact from Oracle
2. **Upsert key** to prevent duplicate contact creation
3. **Reference field** for data reconciliation and troubleshooting

---

## Impact

**Without this field, we cannot:**
- Load 10,000 contacts from Oracle
- Prevent duplicate contact creation on subsequent loads
- Track which Oracle workers are in Salesforce
- Validate AccountContactRelation (ACR) relationships
- Generate reconciliation reports

**Current workaround:** None - field is mandatory for data integrity

---

## Reference

The Account object already has this field configured correctly:
```
Object: Account
Field: External_Id__c
Type: Text(18)
External ID: Yes
Unique: Yes
Status: ✅ Working
```

We need the same configuration on the Contact object.

---

## Next Steps

1. **Salesforce Admin:** Create External_Id__c field on Contact object in SIT
2. **Data Team:** Will re-run contact load script once field is available
3. **Expected Timeline:** 10,000 contacts ready to load immediately after field creation

---

## Contact

For questions or confirmation when field is ready:
- **Data Team:** Anvesh Cherupalli
- **Scripts Ready:** sit_contact_load.py, sit_contact_tests.py
- **Documentation:** SIT_CONTACT_MAPPING.csv in mappings/ folder
