Subject: Field Officer Email Addresses - Data Migration Query

Hi [Oracle Admin Name],

Quick question about Field Officer user data in Oracle.

We're migrating Field Officer assignments to Salesforce and need email addresses for the users. Currently querying CO_USER table:

```sql
SELECT 
    USER_CODE,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,  -- This column appears to be NULL/empty
    IS_ACTIVE
FROM SCH_CO_20.CO_USER
WHERE USER_CODE IN (
    SELECT DISTINCT ASSIGNED_TO 
    FROM SCH_CO_20.CO_FIELD_OFFICER_VISIT
)
```

**Question:** 
- Does CO_USER.EMAIL contain valid email addresses for these 47 Field Officers?
- If not, is there another table/column where Field Officer emails are stored?
- Are these users' emails available in the system at all?

**Field Officers we need emails for:**
- MICHAELD (Michael Docherty) - Active
- JEREMYT (Jeremy Tobin) - Active
- PETERGIU (Peter Giudice) - Inactive
- ... (47 total)

**Why we need this:**
Salesforce requires unique email addresses for all users. Currently creating synthetic emails like `michael.docherty.sit2@leaveplus.com.au`, but if Oracle has the actual emails, we'd prefer to use those.

**Current workaround:**
If Oracle doesn't have emails, we'll continue using generated emails with .sit2 suffix for the SIT environment.

Please let me know:
1. Whether CO_USER.EMAIL has data for Field Officers
2. If there's an alternate email source
3. If we should proceed with generated emails

Thanks!
Anvesh

---

**ALTERNATE APPROACH - If Oracle admin unavailable:**

We can also check if production Salesforce already has these Field Officers as users:
- If yes, we can copy their production emails
- If no, we'll use the generated .sit2 format (safe for testing)

Do you want me to:
A) Send the above message to Oracle admin and wait
B) Check production Salesforce for existing Field Officer users
C) Proceed with .sit2 generated emails (current approach)
