## Data Loading to Country-Specific Tables in Snowflake

This Python script loads data from a `.txt` file into Snowflake tables that are dynamically created based on the country specified in the data. It uses `pandas` for data manipulation and `SQLAlchemy` for connecting to Snowflake. The data is first validated, transformed, and filtered before being inserted into the respective country tables.

## Prerequisites

Before running the script, ensure that you have the following installed:

- Python 3.x
- `pandas` for data manipulation
- `sqlalchemy` for connecting to Snowflake
- `snowflake-sqlalchemy` for Snowflake SQLAlchemy support
- `python-dotenv` for loading environment variables from a `.env` file

Install the required Python packages:

```bash
pip install pandas sqlalchemy snowflake-sqlalchemy python-dotenv
```

You will also need to set up a `.env` file with the following variables:

```
SNOWFLAKE_USER=<your_snowflake_username>
SNOWFLAKE_PASSWORD=<your_snowflake_password>
SNOWFLAKE_ACCOUNT=<your_snowflake_account>
SNOWFLAKE_WAREHOUSE=<your_snowflake_warehouse>
SNOWFLAKE_DATABASE=<your_snowflake_database>
SNOWFLAKE_SCHEMA=<your_snowflake_schema>
SNOWFLAKE_ROLE=<your_snowflake_role>
```

## File Format

The script expects the input file to be a `.txt` file with a `|` delimiter. The data should have the following columns:

- `Record_Type`: Indicates whether the record is a header (`H`) or data (`D`).
- `Customer_Name`: Name of the customer.
- `CustomerID`: Unique identifier for the customer.
- `CustomerOpenDate`: Date the customer was created in `YYYYMMDD` format.
- `LastConsultedDate`: Last consultation date in `YYYYMMDD` format.
- `VaccinationType`: Type of vaccination.
- `Doctor`: Name of the doctor.
- `State`: The state or region of the customer.
- `Country`: The country of the customer.
- `PostCode`: Postal code.
- `DateofBirth`: Date of birth in `DDMMYYYY` format.
- `ActiveCustomer`: A status indicator for whether the customer is active (`A` for active).

Example of data format:

```
H|Customer_Name|CustomerID|CustomerOpenDate|LastConsultedDate|VaccinationType|Doctor|State|Country|PostCode|DateofBirth|ActiveCustomer
D|Yael|1|20101012|20121013|sput5|Piper Sheppard|Lange| |75532|06031987|A
D|Xaviera|2|20101112|20210329|sput5|Savannah Keith|Sint|Peru|3604|26011999|A
D|Matthew|3|20100323|20160202|sput5|Justin Leblanc|Marlb|Peru|18518|17121996|A
```

## Script Overview

### `load_data_to_country_tables(file_path, conn)`

This function does the following:
1. Reads the input file (`file_path`) into a pandas DataFrame.
2. Validates that the file has the correct number of columns.
3. Filters out the header records (`Record_Type` == 'H') and focuses on the data records (`Record_Type` == 'D').
4. Cleans the data by:
   - Removing records with missing or empty country values.
   - Normalizing the country names to title case.
   - Converting date columns to `datetime` format.
5. Dynamically loads the data into Snowflake tables, creating country-specific table names like `Table_<Country>` and appending the data.

### `snow_connector(account, user, password, database, schema, warehouse, role)`

This function connects to Snowflake using the provided credentials and returns a connection object.

### `main()`

The main function does the following:
1. Loads environment variables from the `.env` file.
2. Establishes a connection to Snowflake.
3. Loads the data from the specified file into the country-specific tables in Snowflake.
4. Closes the connection after the data load is complete.

## Running the Script

1. Create a `.env` file with your Snowflake credentials and save it in the root of your project directory.
2. Place your input `.txt` file (e.g., `hospital_data.txt`) in the `data/` directory (or update the `file_path` variable accordingly).
3. Run the script using the following command:

```bash
python snowflake-etl..py
```

This will load the data into your Snowflake country-specific tables.

## Error Handling

The script includes error handling for the following scenarios:
- File reading issues (e.g., bad lines).
- Invalid column formats.
- Date conversion errors.
- Snowflake connection or data loading errors.


```

Make sure to update the `file_path` variable to match the location of your data file when running the script.
