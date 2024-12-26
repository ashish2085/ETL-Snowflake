import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import sys

def load_data_to_staging(file_path, conn):
    try:
        # Read the file with '|' delimiter and skip the first header row
        df = pd.read_csv(file_path, delimiter="|", header=0, on_bad_lines='warn')
        
        # Check the number of columns to ensure consistency
        expected_columns = ['Record_Type', 'Customer_Name', 'CustomerID', 'CustomerOpenDate', 
                            'LastConsultedDate', 'VaccinationType', 'Doctor', 'State', 'Country', 
                            'PostCode', 'DateofBirth', 'ActiveCustomer']
        
        if df.shape[1] != len(expected_columns):
            print(f"Warning: Some rows do not have the expected number of columns (Expected {len(expected_columns)}, got {df.shape[1]})")
        
        # Assign column names
        df.columns = expected_columns
        
        # Filter only the detail records (those with 'D' in the 'Record_Type' column)
        df_detail = df[df['Record_Type'] == 'D']
        
        # Converting date columns to datetime format
        try:
            df_detail['CustomerOpenDate'] = pd.to_datetime(df_detail['CustomerOpenDate'], format='%Y%m%d', errors='coerce')
            df_detail['LastConsultedDate'] = pd.to_datetime(df_detail['LastConsultedDate'], format='%Y%m%d', errors='coerce')
            df_detail['DateofBirth'] = pd.to_datetime(df_detail['DateofBirth'], format='%d%m%Y', errors='coerce')
        except Exception as e:
            print(f"Error converting dates: {e}")
        
        # Check for invalid data or NaT in date columns
        if df_detail[['CustomerOpenDate', 'LastConsultedDate', 'DateofBirth']].isna().sum().any():
            print("Warning: Some date fields could not be converted and are set to NaT.")

        # Insert data into the staging table
        df_detail.to_sql('Staging_Customers', conn, if_exists='append', index=False)
        
        print(f"Data loaded successfully into Staging_Customers table.")
    except Exception as e:
        print(f"Error loading data to staging: {e}")
        sys.exit(1)

def snow_connector(account, user, password, database, schema, warehouse, role):
    try:
        # Connect to Snowflake using SQLAlchemy
        engine = create_engine(URL(
            account=account, 
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role
        ))
        conn = engine.connect()
        return engine, conn
    except Exception as e:
        print(f"Connection error: {e}")
        sys.exit(1)

def main():
    # Load environment variables
    load_dotenv()
    user = str(os.getenv('SNOWFLAKE_USER'))
    password = str(os.getenv('SNOWFLAKE_PASSWORD'))
    account = str(os.getenv('SNOWFLAKE_ACCOUNT'))
    warehouse = str(os.getenv('SNOWFLAKE_WAREHOUSE'))
    database = str(os.getenv('SNOWFLAKE_DATABASE'))
    schema = str(os.getenv('SNOWFLAKE_SCHEMA'))
    role = str(os.getenv('SNOWFLAKE_ROLE'))
    
    # Connect to Snowflake
    engine, conn = snow_connector(account, user, password, database, schema, warehouse, role)
    
    # Path to the uploaded .txt file
    file_path = 'data/hospital_data.txt'
    
    # Load data into staging table
    load_data_to_staging(file_path, conn)
    
    # Close the connection
    conn.close()
    engine.dispose()

if __name__ == "__main__":
    main()
