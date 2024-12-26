
```markdown
# Snowflake Data Loader

This Python script loads customer data from a pipe-delimited file into a Snowflake staging table. It processes the file, converts necessary columns (especially dates), and inserts the data into the Snowflake database.

## Prerequisites

Before running the script, make sure you have the following:

- **Python** 3.x installed.
- **Snowflake account** and credentials (username, password, account, warehouse, database, schema, role).
- **Pandas** library for data manipulation.
- **SQLAlchemy** and **Snowflake SQLAlchemy** for Snowflake connection.
- **dotenv** for environment variable management.
- **Snowflake staging table** (`Staging_Customers`) already created in your database.

### Install Dependencies

First, install the necessary Python dependencies. You can do this by running the following command:

```bash
pip install pandas sqlalchemy snowflake-sqlalchemy python-dotenv
```

## Environment Setup

1. **Create a `.env` file**: This file will store your Snowflake credentials securely. Ensure it is in the same directory as the Python script. Here's an example of the `.env` file:

   ```env
   SNOWFLAKE_USER=<your_snowflake_user>
   SNOWFLAKE_PASSWORD=<your_snowflake_password>
   SNOWFLAKE_ACCOUNT=<your_snowflake_account>
   SNOWFLAKE_WAREHOUSE=<your_snowflake_warehouse>
   SNOWFLAKE_DATABASE=<your_snowflake_database>
   SNOWFLAKE_SCHEMA=<your_snowflake_schema>
   SNOWFLAKE_ROLE=<your_snowflake_role>
   ```

   Replace the placeholders with your actual Snowflake credentials.

2. **Staging Table in Snowflake**: Ensure that the staging table (`Staging_Customers`) exists in the Snowflake schema and matches the columns expected by the script. The required columns should be:

   - `Record_Type`
   - `Customer_Name`
   - `CustomerID`
   - `CustomerOpenDate`
   - `LastConsultedDate`
   - `VaccinationType`
   - `Doctor`
   - `State`
   - `Country`
   - `PostCode`
   - `DateofBirth`
   - `ActiveCustomer`

   Example SQL to create the table:

   ```sql
   CREATE OR REPLACE TABLE Staging_Customers (
       Record_Type STRING,
       Customer_Name STRING,
       CustomerID STRING,
       CustomerOpenDate DATE,
       LastConsultedDate DATE,
       VaccinationType STRING,
       Doctor STRING,
       State STRING,
       Country STRING,
       PostCode STRING,
       DateofBirth DATE,
       ActiveCustomer STRING
   );
   ```

## Running the Script

1. **Prepare Your Data File**: Ensure your data file is in the correct format and available at the location specified in the `file_path` variable in the script (default is `'data/hospital_data.txt'`).

   Example of the expected format (pipe `|` delimited):

   ```
   H|Customer_Name|CustomerID|CustomerOpenDate|LastConsultedDate|VaccinationType|Doctor|State|Country|PostCode|DateofBirth|ActiveCustomer
   D|Yael|1|20101012|20121013|sput5|Piper Sheppard|Lange| |75532|06031987|A
   D|Xaviera|2|20101112|20210329|sput5|Savannah Keith|Sint|Peru|3604|26011999|A
   D|Matthew|3|20100323|20160202|sput5|Justin Leblanc|Marlb|Peru|18518|17121996|A
   ```

2. **Run the Script**: After setting up the environment and ensuring the data file is ready, you can run the script by executing:

   ```bash
   python load_to_snowflake.py
   ```

   This will:
   - Read the data from the file.
   - Parse and transform the necessary columns (especially date columns).
   - Insert the valid records into the Snowflake staging table (`Staging_Customers`).

## Script Overview

1. **`load_data_to_staging`**: This function reads the pipe-delimited `.txt` file, processes the data (including date conversion), and inserts the valid rows into the Snowflake staging table.

2. **`snow_connector`**: This function handles the connection to Snowflake using SQLAlchemy and the Snowflake SQLAlchemy connector. It establishes the connection and returns the engine and connection object.

3. **Environment Variables**: The script uses environment variables stored in a `.env` file for Snowflake credentials. This keeps your sensitive credentials secure and avoids hardcoding them into the script.

## Error Handling

- **Invalid Date Formats**: Any invalid date fields (such as `20201232` in `CustomerOpenDate`) will be converted to `NaT`. You will receive a warning message in the terminal.
- **Missing Columns**: If the data file has missing or extra columns compared to the expected format, the script will issue a warning.

## Troubleshooting

- **Missing Dependencies**: If you get errors about missing packages, run `pip install -r requirements.txt` to install them.
- **Connection Errors**: Ensure your Snowflake credentials in the `.env` file are correct and that your network allows access to Snowflake.
