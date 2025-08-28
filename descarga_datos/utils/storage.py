import csv
import sqlite3
import os
from typing import List, Dict, Any, Union
import logging
import pandas as pd
import json

logger = logging.getLogger(__name__)

def save_to_csv(data: Union[pd.DataFrame, List[Dict[str, Any]]], file_path: str) -> bool:
    """
    Save data to a CSV file.
    
    Args:
        data: List of dictionaries or DataFrame representing the data rows.
        file_path: Path to the CSV file.
        
    Returns:
        True if successful, False otherwise.
    """
    if isinstance(data, pd.DataFrame):
        df = data.copy()
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        data = df.to_dict(orient='records')

    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            if data and len(data) > 0:
                fieldnames = data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            else:
                logger.warning("No data to save to CSV.")
        logger.info(f"Data saved to CSV: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving to CSV {file_path}: {e}")
        return False

def save_to_sqlite(data: Union[pd.DataFrame, List[Dict[str, Any]]], table_name: str, db_path: str) -> bool:
    """
    Save data to a SQLite database.
    
    Args:
        data: List of dictionaries or DataFrame representing the data rows.
        table_name: Name of the table to insert into.
        db_path: Path to the SQLite database file.
        
    Returns:
        True if successful, False otherwise.
    """
    if isinstance(data, pd.DataFrame):
        df = data.copy()
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        data = df.to_dict(orient='records')

    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        if data and len(data) > 0:
            # Create table if not exists based on first row keys
            first_row = data[0]
            columns = ', '.join([f'"{key}" TEXT' for key in first_row.keys()])
            create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})'
            cursor.execute(create_table_sql)
            
            # Insert data
            placeholders = ', '.join(['?' for _ in first_row.keys()])
            columns_names = ', '.join([f'"{key}"' for key in first_row.keys()])
            insert_sql = f'INSERT INTO "{table_name}" ({columns_names}) VALUES ({placeholders})'
            for row in data:
                values = list(row.values())
                for i, value in enumerate(values):
                    if isinstance(value, (dict, list)):
                        values[i] = json.dumps(value)
                cursor.execute(insert_sql, values)
            
            conn.commit()
        else:
            logger.warning("No data to save to SQLite.")
        
        conn.close()
        logger.info(f"Data saved to SQLite: {db_path}, table: {table_name}")
        return True
    except Exception as e:
        logger.error(f"Error saving to SQLite {db_path}: {e}")
        return False

# Additional functions for specific data types can be added here