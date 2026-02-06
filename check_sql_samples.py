"""
Check sample values and field lengths from SQL Server ABR data
"""

import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd

load_dotenv('.env.sit')

print("\n" + "="*80)
print("SQL SERVER ABR DATA - SAMPLE VALUES & LENGTH ANALYSIS")
print("="*80)

try:
    sql_server = 'cosql-test.coinvest.com.au'
    database = 'AvatarWarehouse'
    
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={sql_server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    
    conn = pyodbc.connect(conn_str)
    
    # First, get list of all columns
    print("\n[0] Available Columns in abr_cleaned table:")
    print("-" * 80)
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'datascience'
        AND TABLE_NAME = 'abr_cleaned'
        ORDER BY ORDINAL_POSITION
    """)
    
    columns = cursor.fetchall()
    for col_name, data_type in columns:
        print(f"  {col_name:50s} => {data_type}")
    
    print(f"\n  Total columns: {len(columns)}")
    
    # Get sample data
    query = """
    SELECT TOP 20
        [Australian Business Number] as ABN,
        [ABN Registration - Date of Effect] as ABN_Reg_Date,
        [ABN Status] as ABN_Status,
        [Main - Industry Class] as Industry_Class,
        [Main - Industry Class Code] as Industry_Code
    FROM [datascience].[abr_cleaned]
    WHERE [Australian Business Number] IS NOT NULL
    ORDER BY [Australian Business Number]
    """
    
    df = pd.read_sql(query, conn)
    
    print("\n[1] Sample Records (First 20):")
    print("-" * 80)
    print(df.to_string(index=False))
    
    # Get statistics
    stats_query = """
    SELECT 
        COUNT(*) as Total_Records,
        COUNT(ABN) as ABN_Count,
        COUNT([ABN Registration - Date of Effect]) as ABN_Date_Count,
        COUNT([ABN Status]) as Status_Count,
        COUNT([Main - Industry Class]) as Industry_Class_Count,
        COUNT([Main - Industry Class Code]) as Industry_Code_Count,
        MAX(LEN([ABN Status])) as Status_Max_Len,
        MAX(LEN([Main - Industry Class])) as Industry_Class_Max_Len,
        MAX(LEN(CAST([Main - Industry Class Code] AS VARCHAR))) as Industry_Code_Max_Len
    FROM [datascience].[abr_cleaned]
    """
    
    cursor = conn.cursor()
    cursor.execute(stats_query)
    stats = cursor.fetchone()
    
    print("\n[2] Field Statistics:")
    print("-" * 80)
    print(f"  Total Records: {stats[0]:,}")
    print(f"  ABN populated: {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")
    print(f"  ABN Registration Date populated: {stats[2]:,} ({stats[2]/stats[0]*100:.1f}%)")
    print(f"  ABN Status populated: {stats[3]:,} ({stats[3]/stats[0]*100:.1f}%)")
    print(f"  Industry Class populated: {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)")
    print(f"  Industry Code populated: {stats[5]:,} ({stats[5]/stats[0]*100:.1f}%)")
    print(f"\n  Max Field Lengths:")
    print(f"    ABN Status: {stats[6]} characters")
    print(f"    Industry Class: {stats[7]} characters")
    print(f"    Industry Code: {stats[8]} characters")
    
    # Get distinct picklist values
    picklist_query = """
    SELECT DISTINCT [ABN Status] as Value
    FROM [datascience].[abr_cleaned]
    WHERE [ABN Status] IS NOT NULL
    """
    cursor.execute(picklist_query)
    statuses = [row[0] for row in cursor.fetchall()]
    
    print("\n[3] ABN Status - Distinct Values (Picklist):")
    print("-" * 80)
    for i, status in enumerate(sorted(statuses)[:20], 1):
        print(f"  {i:2d}. {status}")
    if len(statuses) > 20:
        print(f"  ... and {len(statuses) - 20} more values")
    print(f"  Total distinct values: {len(statuses)}")
    
    # Get sample industry classes
    industry_query = """
    SELECT DISTINCT TOP 20 [Main - Industry Class] as Value
    FROM [datascience].[abr_cleaned]
    WHERE [Main - Industry Class] IS NOT NULL
    ORDER BY [Main - Industry Class]
    """
    cursor.execute(industry_query)
    industries = [row[0] for row in cursor.fetchall()]
    
    print("\n[4] Industry Class - Sample Values (First 20):")
    print("-" * 80)
    for i, industry in enumerate(industries, 1):
        print(f"  {i:2d}. {industry}")
    
    # Get sample industry codes
    code_query = """
    SELECT DISTINCT TOP 20 [Main - Industry Class Code] as Value
    FROM [datascience].[abr_cleaned]
    WHERE [Main - Industry Class Code] IS NOT NULL
    ORDER BY [Main - Industry Class Code]
    """
    cursor.execute(code_query)
    codes = [row[0] for row in cursor.fetchall()]
    
    print("\n[5] Industry Class Code - Sample Values (First 20):")
    print("-" * 80)
    for i, code in enumerate(codes, 1):
        print(f"  {i:2d}. {code}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*80)
    print("✅ ANALYSIS COMPLETE")
    print("="*80)
    print("\nRECOMMENDATIONS:")
    print("  1. ABN Status: Use as picklist (limited distinct values)")
    print("  2. Industry Class: Use as picklist (likely limited ANZSIC codes)")
    print(f"  3. Industry Code: VARCHAR({stats[8]}) → SF Text({50}) - check if sufficient")
    print("  4. ABN Registration Date: date → date (compatible)")
    print("="*80)
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
