# FieldOfficerAllocated__c - Issue & Resolution

**Date:** February 10, 2026  
**Status:** BLOCKED - Awaiting User Creation  
**Impact:** 1,698 contacts (0.96%) need field officer assignments

---

## The Issue

**FieldOfficerAllocated__c** is a Lookup(User) field on Contact that cannot be populated because:

1. **Field officer users don't exist** in Salesforce test/SIT environment
2. **Lookup(User) requires valid Salesforce User IDs** - can't lookup users that don't exist
3. We're loading 176,536 contacts but can't assign their field officers

---

## What We Have (Oracle)

Oracle has **47 field officer codes** with **1,698 worker assignments**:

| Officer Code | Workers Assigned |
|--------------|------------------|
| MICHAELD | 1,150 |
| JEREMYT | 494 |
| PETERGIU | 404 |
| MATTMCPH | 208 |
| ANDREWMAC | 173 |
| ANDREWMA | 114 |
| ... | ... |
| **Total: 47 officers** | **1,698 workers** |

**⚠️ Oracle Only Has Codes** - No full names, emails, or contact details

---

## What We Need

### 1. Field Officer Details (From Field Operations Manager/HR)

Need full details for all 47 officers:

```
Officer Code → Full Name, Email Address
MICHAELD → Michael Docherty, michael.docherty@company.com
JEREMYT → Jeremy Thompson, jeremy.t@company.com
PETERGIU → Peter Giuliano, peter.giu@company.com
... (44 more)
```

**Action:** Send list of 47 officer codes to Field Ops Manager/HR

### 2. Salesforce User Creation (Salesforce Administrator)

Once details received, SF Admin needs to:

- Create 47 User records in Salesforce
- Assign appropriate licenses and profiles
- Configure mobile app permissions
- Username must match Oracle codes (e.g., `MICHAELD@company.com`)

**Action:** SF Admin creates 47 field officer users

### 3. Data Load Script Update (Data Team)

After users created:

- Query Salesforce Users by Username
- Map Oracle ASSIGNED_TO → SF User.Id
- Load FieldOfficerAllocated__c with lookups

**Action:** Update contact load script to include field officer mapping

---

## Resolution Options

### Option 1: Skip Field for Now ⏸️ **(Current)**
- Don't include FieldOfficerAllocated__c in contact load
- Add field after go-live when users exist
- **Pros:** Clean, no errors
- **Cons:** Field missing until later

### Option 2: Load with NULL 🔵
- Include field in load but set all to NULL
- Update values after users created
- **Pros:** Field structure ready, easy to update later
- **Cons:** 1,698 contacts temporarily missing assignments

### Option 3: Create Users First, Then Load 🟢 **(RECOMMENDED)**
- Get officer details → SF Admin creates users → Load with mappings
- **Pros:** Complete data from day one
- **Cons:** Requires coordination across teams before load

---

## Impact

**If we skip this field:**
- 1,698 contacts won't have field officer assignments
- Field officers can't use mobile app to see assigned workers
- Business impact: No field visit management at go-live

**If we resolve before load:**
- Complete data migration
- Field officers ready to use mobile app
- 47 officers can manage their 1,698 assigned workers immediately

---

## Recommended Action Plan

### Step 1: Get Officer Details (1-2 days)
**Owner:** Data Team  
**Action:** Send 47 officer codes to Field Operations Manager/HR  
**Deliverable:** CSV with Code, Name, Email for all 47 officers

### Step 2: Create SF Users (2-3 days)
**Owner:** Salesforce Administrator  
**Action:** Create 47 User records with proper licenses/profiles  
**Deliverable:** 47 active users in Salesforce

### Step 3: Update Load Script (1 day)
**Owner:** Data Team  
**Action:** Add field officer mapping logic  
**Deliverable:** Updated contact load script with FieldOfficerAllocated__c

### Step 4: Execute Contact Load
**Owner:** Data Team  
**Action:** Run full contact load with all 24 fields  
**Deliverable:** 176,536 contacts loaded with field officer assignments

**Total Timeline:** 4-6 days

---

## Current Status

✅ **Completed:**
- Identified 47 field officers in Oracle
- Analyzed 1,698 worker assignments
- Contact load script ready (23 fields)

⏳ **Pending:**
- Officer details from Field Ops/HR
- SF user creation by SF Admin
- Field officer mapping implementation

🔴 **Blocked:**
- Cannot load FieldOfficerAllocated__c until users exist

---

## Questions?

Contact the Data Team for more information about Oracle data or the contact load process.

Contact the SF Admin team for user creation and Salesforce configuration.
