# Field Officer Implementation

This folder contains all code and data related to the Field Officer functionality implementation in Salesforce SIT.

## Overview

47 Field Officer users created in SIT to support the FieldOfficerAllocated__c lookup field on Contact object.

## Key Files

### User Creation Scripts
- `create_field_officer_users.py` - Main script to create all 47 Field Officer users
- `fix_remaining_8_officers.py` - Fixed 6 officers with duplicate username issues
- `fix_apostrophe_officers.py` - Attempted to fix apostrophe issues (JJO, MOB)
- `create_jjo_mob_sit2.py` - Successfully created JJO and MOB with .sit2 suffix
- `create_veenaloya_sit.py` - Created 47th Field Officer (Veena Loya)
- `fix_remaining_officers.py` - Legacy script for PROTO environment

### Data Files
- `field_officer_salesforce_mapping.csv` - **MASTER FILE** - Maps Oracle officer codes to SF User IDs (47 officers)
  - **Note**: Copy also exists in root directory for `sit_contact_load.py` to access
  - Keep both copies synchronized when updating
- `field_officers_from_oracle.csv` - Original export from Oracle CO_USER table
- `field_officers_priority_list.csv` - Active officers prioritized by worker count
- `field_officer_real_emails_TEMPLATE.csv` - Template for Cherie to provide real emails

### Contact Load Integration
- `retry_failed_field_officers.py` - Fixed 28 failed contacts after creating users in correct environment

### Analysis & Diagnostics
- `export_field_officers.py` - Export Field Officers from Oracle
- `summary_field_officers.py` - Analyze Field Officer assignments and coverage
- `check_co_user_columns.py` - Verified Oracle CO_USER table has no EMAIL column
- `check_oracle_field_officer_emails.py` - Attempted to check for emails in Oracle
- `check_user_visibility.py` - Diagnosed user visibility issues
- `verify_user_existence.py` - Checked if users exist in Salesforce
- `diagnose_jjo_error.py` - Diagnosed JJO user creation error
- `get_michaeld_user.py` - Retrieved MICHAELD user details
- `search_michaeld.py` - Searched for MICHAELD user

### Utility Scripts
- `update_field_officer_emails.py` - Script to update all users with real emails (run after receiving from Cherie)

### Documentation
- `FIELD_OFFICER_ISSUE.md` - Issue tracking and resolution notes

## Implementation Summary

### Environment Issue Resolution
- **Problem**: Initially created 47 users in PROTO environment by mistake
- **Root Cause**: `create_field_officer_users.py` used `.env` instead of `.env.sit`
- **Solution**: Re-created all 47 users in SIT with `.sit2` email suffix
- **Result**: All 50,000 contacts loaded successfully, 28 Field Officer assignments working

### Email Address Strategy
- Oracle CO_USER table has no EMAIL column
- Generated synthetic emails: `firstname.lastname.sit2@leaveplus.com.au`
- `.sit2` suffix required because `.sit` already used in PROTO
- SF Admin confirmed: Inactive users can be referenced in lookups

### User Distribution
- **Total**: 47 Field Officers
- **Active**: 11 officers (MICHAELD, JEREMYT, WJS, AH, BAW, CA, CAA, CJW, DMG, EUH, GJC)
- **Inactive**: 36 officers
- **Top 2**: MICHAELD (784 workers), JEREMYT (311 workers) = 62% of assignments

### Coverage Statistics
- **50K batch**: 28 contacts (0.1%) have Field Officer assignments
- **Full database**: 1,754 contacts (0.99% of 176K active workers)
- **Active officers**: Handle 1,096 contacts (62.5% of assignments)
- **Inactive officers**: 658 contacts (historical data)

## Key Learnings

1. **Always use `.env.sit` for SIT scripts** - Environment-specific environment files critical
2. **Salesforce usernames are globally unique** - Must use unique suffixes for test environments
3. **Inactive users can be referenced** - SF Admin confirmed no restrictions on inactive user lookups
4. **Oracle has no email data** - Field Officer emails not stored in system
5. **User visibility matters** - Users must exist in same environment for lookups to work

## Next Steps (Optional)

1. Request real email addresses from Cherie
2. Update template: `field_officer_real_emails_TEMPLATE.csv`
3. Run: `python update_field_officer_emails.py`
4. Users can then login with real email addresses

## Contact Load Integration

The contact load script (`sit/sit_contact_load.py`) includes:
- Lines 27-38: Load Field Officer mapping
- Lines 150-162: SQL subquery to extract FIELD_OFFICER_CODE
- Lines 402-405: Map Oracle code to SF User ID
- Lines 432-437: Field Officer assignment statistics

All 50,000 contacts loaded successfully with FieldOfficerAllocated__c field populated for eligible contacts.
