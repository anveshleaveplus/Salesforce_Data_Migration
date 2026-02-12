"""
Find Return and Claim tables in Oracle SCH_CO_20 schema
"""
import os
import oracledb
from dotenv import load_dotenv

load_dotenv()

# Connect to Oracle
connection = oracledb.connect(
    user=os.getenv('ORACLE_USER'),
    password=os.getenv('ORACLE_PASSWORD'),
    host=os.getenv('ORACLE_HOST'),
    port=int(os.getenv('ORACLE_PORT')),
    sid=os.getenv('ORACLE_SID')
)

cursor = connection.cursor()

print("=" * 100)
print("ORACLE TABLES - RETURN AND CLAIM RELATED")
print("=" * 100)

# Search for tables with RETURN or CLAIM in the name
query = """
    SELECT table_name, num_rows
    FROM all_tables
    WHERE owner = 'SCH_CO_20'
    AND (table_name LIKE '%RETURN%' OR table_name LIKE '%CLAIM%' OR table_name LIKE '%BENEFIT%')
    ORDER BY table_name
"""

cursor.execute(query)
tables = cursor.fetchall()

print(f"\nFound {len(tables)} tables with RETURN/CLAIM/BENEFIT:\n")
for table_name, num_rows in tables:
    row_count = f"{num_rows:,}" if num_rows else "Unknown"
    print(f"  • {table_name:<50} ({row_count} rows)")

# Get column details for each table
for table_name, _ in tables:
    print("\n" + "=" * 100)
    print(f"TABLE: {table_name}")
    print("=" * 100)
    
    # Get columns
    col_query = """
        SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
        FROM all_tab_columns
        WHERE owner = 'SCH_CO_20'
        AND table_name = :table_name
        ORDER BY column_id
    """
    
    cursor.execute(col_query, table_name=table_name)
    columns = cursor.fetchall()
    
    print(f"\n{len(columns)} columns:")
    print(f"{'Column Name':<40} {'Type':<20} {'Nullable':<10}")
    print("-" * 100)
    
    for col_name, data_type, data_length, data_precision, data_scale, nullable in columns:
        # Format data type
        if data_type == 'NUMBER':
            if data_precision:
                type_str = f"NUMBER({data_precision},{data_scale})"
            else:
                type_str = "NUMBER"
        elif data_type in ('VARCHAR2', 'CHAR'):
            type_str = f"{data_type}({data_length})"
        else:
            type_str = data_type
        
        null_str = "Yes" if nullable == 'Y' else "No"
        print(f"{col_name:<40} {type_str:<20} {null_str:<10}")
    
    # Get sample record count
    try:
        count_query = f"SELECT COUNT(*) FROM SCH_CO_20.{table_name}"
        cursor.execute(count_query)
        count = cursor.fetchone()[0]
        print(f"\nTotal records: {count:,}")
        
        # Show a sample record if exists
        if count > 0:
            sample_query = f"SELECT * FROM SCH_CO_20.{table_name} WHERE ROWNUM = 1"
            cursor.execute(sample_query)
            sample = cursor.fetchone()
            print("\nSample record (first row):")
            col_names = [desc[0] for desc in cursor.description]
            for i, col_name in enumerate(col_names):
                value = sample[i]
                if value is not None:
                    print(f"  {col_name}: {value}")
    except Exception as e:
        print(f"\nError querying table: {e}")

cursor.close()
connection.close()

print("\n" + "=" * 100)
