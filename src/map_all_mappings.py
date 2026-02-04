#!/usr/bin/env python3
"""
Oracle to Salesforce Account Field Mapping Tool

This script connects to an Oracle database and Salesforce, reads schema/field metadata,
generates pairwise mappings with explainable scores, and outputs results to various formats.
"""

import os
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import oracledb
import pandas as pd
from dotenv import load_dotenv
from rapidfuzz import fuzz
from simple_salesforce import Salesforce
from tqdm import tqdm
import json
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
OUTPUT_DIR = Path('out')
WEIGHTS = {
    'name': 0.60,
    'dtype': 0.20,
    'pattern': 0.15,
    'synonym': 0.05
}

# Synonym dictionary for bonus scoring
SYNONYMS = {
    'id': ['identifier', 'key', 'code'],
    'name': ['title', 'label', 'description'],
    'email': ['mail', 'e-mail'],
    'phone': ['telephone', 'mobile', 'contact'],
    'address': ['location', 'addr'],
    'city': ['town', 'municipality'],
    'state': ['province', 'region'],
    'zip': ['postcode', 'postal_code'],
    'country': ['nation', 'territory'],
    'website': ['url', 'site', 'domain'],
    'industry': ['sector', 'field'],
    'revenue': ['income', 'sales'],
    'employees': ['staff', 'workforce']
}

def load_environment() -> None:
    """Load environment variables from .env file."""
    load_dotenv()
    logger.info("Environment variables loaded.")

def get_oracle_connection() -> oracledb.Connection:
    """Establish connection to Oracle database."""
    try:
        client_lib_dir = os.getenv('ORACLE_CLIENT_LIB_DIR')
        if client_lib_dir and os.path.exists(client_lib_dir):
            oracledb.init_oracle_client(lib_dir=client_lib_dir)
        else:
            logger.warning("ORACLE_CLIENT_LIB_DIR not set or invalid. Using thin mode.")
        
        connection = oracledb.connect(
            user=os.getenv('ORACLE_USER'),
            password=os.getenv('ORACLE_PASSWORD'),
            dsn=f"{os.getenv('ORACLE_HOST')}:{os.getenv('ORACLE_PORT')}/{os.getenv('ORACLE_SERVICE_NAME')}"
        )
        logger.info("Connected to Oracle database.")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to Oracle: {e}")
        raise

def get_oracle_schema(connection: oracledb.Connection) -> pd.DataFrame:
    """Read Oracle schema from ALL_TAB_COLUMNS."""
    schema = os.getenv('ORACLE_SCHEMA')
    if not schema:
        raise ValueError("ORACLE_SCHEMA not set in environment.")
    
    try:
        query = """
        SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :schema
        ORDER BY TABLE_NAME, COLUMN_ID
        """
        
        df = pd.read_sql(query, connection, params={'schema': schema.upper()})
        
        # Apply table filters
        include_tables = os.getenv('INCLUDE_TABLES')
        exclude_tables = os.getenv('EXCLUDE_TABLES')
        
        if include_tables:
            include_list = [t.strip().upper() for t in include_tables.split(',')]
            df = df[df['TABLE_NAME'].isin(include_list)]
        
        if exclude_tables:
            exclude_list = [t.strip().upper() for t in exclude_tables.split(',')]
            df = df[~df['TABLE_NAME'].isin(exclude_list)]
        
        logger.info(f"Retrieved {len(df)} columns from {df['TABLE_NAME'].nunique()} tables.")
        return df
    except Exception as e:
        logger.warning(f"Oracle schema query failed: {e}. Using mock data.")
        # Mock Oracle columns for testing
        mock_data = [
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'CUSTOMER_ID', 'DATA_TYPE': 'NUMBER', 'DATA_LENGTH': None, 'DATA_PRECISION': 10, 'DATA_SCALE': 0, 'NULLABLE': 'N'},
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'CUSTOMER_NAME', 'DATA_TYPE': 'VARCHAR2', 'DATA_LENGTH': 100, 'DATA_PRECISION': None, 'DATA_SCALE': None, 'NULLABLE': 'Y'},
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'EMAIL', 'DATA_TYPE': 'VARCHAR2', 'DATA_LENGTH': 150, 'DATA_PRECISION': None, 'DATA_SCALE': None, 'NULLABLE': 'Y'},
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'PHONE', 'DATA_TYPE': 'VARCHAR2', 'DATA_LENGTH': 20, 'DATA_PRECISION': None, 'DATA_SCALE': None, 'NULLABLE': 'Y'},
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'ADDRESS', 'DATA_TYPE': 'VARCHAR2', 'DATA_LENGTH': 255, 'DATA_PRECISION': None, 'DATA_SCALE': None, 'NULLABLE': 'Y'},
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'CITY', 'DATA_TYPE': 'VARCHAR2', 'DATA_LENGTH': 50, 'DATA_PRECISION': None, 'DATA_SCALE': None, 'NULLABLE': 'Y'},
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'POSTCODE', 'DATA_TYPE': 'VARCHAR2', 'DATA_LENGTH': 10, 'DATA_PRECISION': None, 'DATA_SCALE': None, 'NULLABLE': 'Y'},
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'COUNTRY', 'DATA_TYPE': 'VARCHAR2', 'DATA_LENGTH': 50, 'DATA_PRECISION': None, 'DATA_SCALE': None, 'NULLABLE': 'Y'},
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'INDUSTRY', 'DATA_TYPE': 'VARCHAR2', 'DATA_LENGTH': 100, 'DATA_PRECISION': None, 'DATA_SCALE': None, 'NULLABLE': 'Y'},
            {'OWNER': 'SCH_CO_20', 'TABLE_NAME': 'CUSTOMERS', 'COLUMN_NAME': 'REVENUE', 'DATA_TYPE': 'NUMBER', 'DATA_LENGTH': None, 'DATA_PRECISION': 15, 'DATA_SCALE': 2, 'NULLABLE': 'Y'},
        ]
        df = pd.DataFrame(mock_data)
        logger.info(f"Using mock Oracle data: {len(df)} columns.")
        return df

def sample_column_values(connection: oracledb.Connection, schema: str, table: str, column: str, sample_rows: int) -> List[str]:
    """Sample values from a column for pattern detection."""
    if sample_rows <= 0:
        return []
    
    query = f"SELECT {column} FROM {schema}.{table} WHERE ROWNUM <= {sample_rows} AND {column} IS NOT NULL"
    try:
        df = pd.read_sql(query, connection)
        return df[column].astype(str).tolist()
    except Exception as e:
        logger.warning(f"Failed to sample {schema}.{table}.{column}: {e}")
        return []

def detect_patterns(values: List[str]) -> Dict[str, float]:
    """Detect patterns in sampled values."""
    if not values:
        return {}
    
    patterns = {
        'email': 0,
        'phone': 0,
        'url': 0,
        'postcode': 0,
        'abn': 0  # Australian Business Number
    }
    
    email_regex = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    phone_regex = re.compile(r'^\+?[\d\s\-\(\)]{7,}$')
    url_regex = re.compile(r'^https?://')
    postcode_regex = re.compile(r'^\d{4,5}$')  # Simple postcode pattern
    abn_regex = re.compile(r'^\d{11}$')  # ABN is 11 digits
    
    for value in values:
        value = value.strip()
        if email_regex.match(value):
            patterns['email'] += 1
        if phone_regex.match(value):
            patterns['phone'] += 1
        if url_regex.match(value):
            patterns['url'] += 1
        if postcode_regex.match(value):
            patterns['postcode'] += 1
        if abn_regex.match(value):
            patterns['abn'] += 1
    
    # Normalize to percentage
    total = len(values)
    for key in patterns:
        patterns[key] /= total if total > 0 else 1
    
    return patterns

def get_salesforce_connection() -> Salesforce:
    """Establish connection to Salesforce."""
    try:
        sf = Salesforce(
            username=os.getenv('SF_USERNAME'),
            password=os.getenv('SF_PASSWORD'),
            security_token=os.getenv('SF_SECURITY_TOKEN'),
            domain=os.getenv('SF_DOMAIN', 'login')
        )
        # Test connection with a simple query
        test_query = sf.query("SELECT Id FROM Account LIMIT 1")
        logger.info("Salesforce connection tested successfully.")
        return sf
    except Exception as e:
        logger.error(f"Failed to connect to Salesforce: {e}")
        raise

def get_salesforce_fields(sf: Salesforce) -> List[Dict]:
    """Get Account field metadata from Salesforce."""
    # For now, using mock fields from screenshots/user input
    # TODO: Replace with actual API call once credentials are available
    mock_fields = [
        {'name': 'Id', 'type': 'id'},
        {'name': 'Name', 'type': 'string'},
        {'name': 'Type', 'type': 'picklist'},
        {'name': 'Industry', 'type': 'picklist'},
        {'name': 'AnnualRevenue', 'type': 'currency'},
        {'name': 'NumberOfEmployees', 'type': 'int'},
        {'name': 'BillingStreet', 'type': 'textarea'},
        {'name': 'BillingCity', 'type': 'string'},
        {'name': 'BillingState', 'type': 'string'},
        {'name': 'BillingPostalCode', 'type': 'string'},
        {'name': 'BillingCountry', 'type': 'string'},
        {'name': 'ShippingStreet', 'type': 'textarea'},
        {'name': 'ShippingCity', 'type': 'string'},
        {'name': 'ShippingState', 'type': 'string'},
        {'name': 'ShippingPostalCode', 'type': 'string'},
        {'name': 'ShippingCountry', 'type': 'string'},
        {'name': 'Phone', 'type': 'phone'},
        {'name': 'Website', 'type': 'url'},
        {'name': 'AccountNumber', 'type': 'string'},
        {'name': 'Sic', 'type': 'string'},
        {'name': 'TickerSymbol', 'type': 'string'},
        {'name': 'Ownership', 'type': 'picklist'},
        {'name': 'Rating', 'type': 'picklist'},
        {'name': 'Site', 'type': 'string'},
        {'name': 'AccountSource', 'type': 'picklist'},
        {'name': 'Description', 'type': 'textarea'},
        # Add more from screenshots as needed
    ]
    logger.info(f"Using mock Salesforce fields: {len(mock_fields)}")
    return mock_fields

def map_oracle_dtype_to_bucket(dtype: str) -> str:
    """Map Oracle data type to compatibility bucket."""
    dtype = dtype.upper()
    if 'VARCHAR' in dtype or 'CHAR' in dtype:
        return 'string'
    elif 'NUMBER' in dtype or 'INT' in dtype or 'FLOAT' in dtype:
        return 'numeric'
    elif 'DATE' in dtype or 'TIMESTAMP' in dtype:
        return 'datetime'
    elif 'CLOB' in dtype or 'BLOB' in dtype:
        return 'large_object'
    else:
        return 'other'

def map_salesforce_dtype_to_bucket(dtype: str) -> str:
    """Map Salesforce data type to compatibility bucket."""
    dtype = dtype.lower()
    if 'string' in dtype or 'textarea' in dtype or 'picklist' in dtype or 'multipicklist' in dtype:
        return 'string'
    elif 'double' in dtype or 'currency' in dtype or 'percent' in dtype or 'int' in dtype:
        return 'numeric'
    elif 'date' in dtype or 'datetime' in dtype:
        return 'datetime'
    elif 'boolean' in dtype:
        return 'boolean'
    elif 'reference' in dtype:
        return 'reference'
    else:
        return 'other'

def calculate_name_score(oracle_name: str, sf_name: str) -> float:
    """Calculate name similarity score."""
    return fuzz.token_set_ratio(oracle_name.lower(), sf_name.lower()) / 100.0

def calculate_dtype_score(oracle_dtype: str, sf_dtype: str) -> float:
    """Calculate data type compatibility score."""
    oracle_bucket = map_oracle_dtype_to_bucket(oracle_dtype)
    sf_bucket = map_salesforce_dtype_to_bucket(sf_dtype)
    return 1.0 if oracle_bucket == sf_bucket else 0.0

def calculate_pattern_bonus(patterns: Dict[str, float], sf_name: str, sf_dtype: str) -> float:
    """Calculate pattern-based bonus."""
    sf_name_lower = sf_name.lower()
    sf_bucket = map_salesforce_dtype_to_bucket(sf_dtype)
    
    bonus = 0.0
    if 'email' in sf_name_lower and patterns.get('email', 0) > 0.5:
        bonus += patterns['email']
    if 'phone' in sf_name_lower and patterns.get('phone', 0) > 0.5:
        bonus += patterns['phone']
    if ('website' in sf_name_lower or 'url' in sf_name_lower) and patterns.get('url', 0) > 0.5:
        bonus += patterns['url']
    if 'postal' in sf_name_lower and patterns.get('postcode', 0) > 0.5:
        bonus += patterns['postcode']
    if 'abn' in sf_name_lower and patterns.get('abn', 0) > 0.5:
        bonus += patterns['abn']
    
    # Additional: if string field and high pattern match
    if sf_bucket == 'string' and any(p > 0.5 for p in patterns.values()):
        bonus += max(patterns.values())
    
    return min(bonus, 1.0)  # Cap at 1.0

def calculate_synonym_bonus(oracle_name: str, sf_name: str) -> float:
    """Calculate synonym-based bonus."""
    oracle_words = set(oracle_name.lower().split('_'))
    sf_words = set(sf_name.lower().split('_'))
    
    for word in oracle_words:
        if word in SYNONYMS:
            synonyms = set(SYNONYMS[word])
            if sf_words & synonyms:
                return 1.0
    
    for word in sf_words:
        if word in SYNONYMS:
            synonyms = set(SYNONYMS[word])
            if oracle_words & synonyms:
                return 1.0
    
    return 0.0

def calculate_total_score(name_score: float, dtype_score: float, pattern_bonus: float, synonym_bonus: float) -> float:
    """Calculate weighted total score."""
    return (
        WEIGHTS['name'] * name_score +
        WEIGHTS['dtype'] * dtype_score +
        WEIGHTS['pattern'] * pattern_bonus +
        WEIGHTS['synonym'] * synonym_bonus
    )

def generate_mappings(oracle_df: pd.DataFrame, sf_fields: List[Dict], connection: oracledb.Connection) -> pd.DataFrame:
    """Generate all pairwise mappings with scores."""
    sample_rows = int(os.getenv('SAMPLE_ROWS', 0))
    schema = os.getenv('ORACLE_SCHEMA').upper()
    
    mappings = []
    
    for _, oracle_row in tqdm(oracle_df.iterrows(), total=len(oracle_df), desc="Processing Oracle columns"):
        oracle_name = oracle_row['COLUMN_NAME']
        oracle_dtype = oracle_row['DATA_TYPE']
        table_name = oracle_row['TABLE_NAME']
        
        # Sample values if enabled
        values = sample_column_values(connection, schema, table_name, oracle_name, sample_rows)
        patterns = detect_patterns(values)
        
        for sf_field in sf_fields:
            sf_name = sf_field['name']
            sf_dtype = sf_field['type']
            
            name_score = calculate_name_score(oracle_name, sf_name)
            dtype_score = calculate_dtype_score(oracle_dtype, sf_dtype)
            pattern_bonus = calculate_pattern_bonus(patterns, sf_name, sf_dtype)
            synonym_bonus = calculate_synonym_bonus(oracle_name, sf_name)
            
            total_score = calculate_total_score(name_score, dtype_score, pattern_bonus, synonym_bonus)
            
            mappings.append({
                'oracle_table': table_name,
                'oracle_column': oracle_name,
                'oracle_dtype': oracle_dtype,
                'sf_table': 'Account',
                'sf_field': f'Account.{sf_name}',
                'sf_dtype': sf_dtype,
                'SCORE': total_score,
                'NAME_SCORE': name_score,
                'DTYPE_SCORE': dtype_score,
                'PATTERN_BONUS': pattern_bonus,
                'SYNONYM_BONUS': synonym_bonus
            })
    
    df = pd.DataFrame(mappings)
    logger.info(f"Generated {len(df)} mappings.")
    return df

def save_outputs(mappings_df: pd.DataFrame) -> None:
    """Save all outputs to files."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # All pairs CSV
    mappings_df.to_csv(OUTPUT_DIR / 'oracle_to_account_all_pairs.csv', index=False)
    
    # Top K per Oracle column
    topk = int(os.getenv('TOPK', 5))
    topk_df = mappings_df.sort_values(['oracle_column', 'SCORE'], ascending=[True, False]).groupby('oracle_column').head(topk)
    topk_df.to_csv(OUTPUT_DIR / 'oracle_to_account_topk.csv', index=False)
    
    # Top K per SF field
    sf_topk_df = mappings_df.sort_values(['sf_field', 'SCORE'], ascending=[True, False]).groupby('sf_field').head(topk)
    sf_topk_df.to_csv(OUTPUT_DIR / 'account_to_oracle_topk.csv', index=False)
    
    # Full YAML
    full_yaml = {}
    for oracle_col in mappings_df['oracle_column'].unique():
        col_mappings = mappings_df[mappings_df['oracle_column'] == oracle_col].sort_values('SCORE', ascending=False)
        full_yaml[oracle_col] = [
            {
                'sf_field': row['sf_field'],
                'score': row['SCORE'],
                'reasons': {
                    'name_similarity': row['NAME_SCORE'],
                    'dtype_compatibility': row['DTYPE_SCORE'],
                    'pattern_bonus': row['PATTERN_BONUS'],
                    'synonym_bonus': row['SYNONYM_BONUS']
                }
            }
            for _, row in col_mappings.iterrows()
        ]
    
    with open(OUTPUT_DIR / 'mappings_full.yaml', 'w') as f:
        yaml.dump(full_yaml, f, default_flow_style=False)
    
    # First choice YAML
    first_choice_yaml = {col: mappings[0]['sf_field'] for col, mappings in full_yaml.items()}
    with open(OUTPUT_DIR / 'mappings.yaml', 'w') as f:
        yaml.dump(first_choice_yaml, f, default_flow_style=False)
    
    logger.info("All outputs saved to 'out/' directory.")

def main() -> None:
    """Main execution function."""
    try:
        load_environment()
        
        # Oracle
        try:
            oracle_conn = get_oracle_connection()
            oracle_schema = get_oracle_schema(oracle_conn)
        except Exception as e:
            logger.warning(f"Oracle connection failed: {e}. Using mock data.")
            oracle_conn = None
            oracle_schema = get_oracle_schema(None)  # Mock data
        
        # Salesforce
        try:
            sf_conn = get_salesforce_connection()
            sf_fields = get_salesforce_fields(sf_conn)
        except Exception as e:
            logger.warning(f"Salesforce connection failed: {e}. Using mock fields.")
            sf_conn = None
            sf_fields = get_salesforce_fields(None)  # Mock fields
        
        # Generate mappings
        mappings_df = generate_mappings(oracle_schema, sf_fields, oracle_conn)
        
        # Save outputs
        save_outputs(mappings_df)
        
        logger.info("Mapping process completed successfully.")
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise
    finally:
        if 'oracle_conn' in locals() and oracle_conn:
            oracle_conn.close()

if __name__ == '__main__':
    main()