"""
Summary of all Field Officer user creation
"""

import csv

print("=" * 80)
print("FIELD OFFICER USER CREATION - FINAL SUMMARY")
print("=" * 80)

# Read mapping
with open('field_officer_salesforce_mapping.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    data = list(reader)

print(f"\n✅ Total Officers: {len(data)}")

# Count by status
created = [r for r in data if 'Created' in r['Status']]
existing = [r for r in data if 'Already Exists' in r['Status']]

print(f"\nBy Creation Status:")
print(f"  • Created Now: {len(created)}")
print(f"  • Already Existed: {len(existing)}")

# Count by active status
active = [r for r in data if r.get('Is_Active') == 'Y']
inactive = [r for r in data if r.get('Is_Active') == 'N']

print(f"\nBy Active Status:")
print(f"  • Active: {len(active)} officers")
print(f"  • Inactive: {len(inactive)} officers")

# Load workers assigned
with open('field_officers_from_oracle.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    oracle_data = {row['Officer_Code']: int(row['Workers_Assigned']) for row in reader}

# Calculate coverage
total_workers = sum(oracle_data.values())
active_workers = sum([oracle_data.get(r['Officer_Code'], 0) for r in active])

print(f"\nContact Coverage:")
print(f"  • Total Field Officer Assignments: {total_workers:,} contacts")
print(f"  • Covered by Active Officers: {active_workers:,} contacts ({active_workers/total_workers*100:.1f}%)")

# Top officers
print(f"\nTop 5 Officers by Assignments:")
top_officers = sorted(data, key=lambda x: oracle_data.get(x['Officer_Code'], 0), reverse=True)[:5]
for officer in top_officers:
    code = officer['Officer_Code']
    user_id = officer['Salesforce_User_Id']
    workers = oracle_data.get(code, 0)
    is_active = officer.get('Is_Active', 'Y')
    status_icon = '✓' if is_active == 'Y' else '✗'
    print(f"  {status_icon} {code}: {workers:,} workers (User ID: {user_id})")

print("\n" + "=" * 80)
print("READY FOR CONTACT LOAD WITH FIELDOFFICERALLOCATED__C")
print("=" * 80)
print("\nMapping file: field_officer_salesforce_mapping.csv")
print("Next step: Update sit_contact_load.py to use this mapping")
