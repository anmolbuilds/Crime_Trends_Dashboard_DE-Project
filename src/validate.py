# Used to parse and validate date-time strings.
from datetime import datetime

# Counter is used to compare the expected and actual DataFrame columns.
from collections import Counter

# Pandas is used for DataFrame operations and handling missing/numeric values.
import pandas as pd

# Prefect task decorator allows validation to be tracked as part of the ETL workflow.
from prefect import task


### UTILITIES

# --------------------------------------------------
# UTILITY 1: SCHEMA VALIDATION
# --------------------------------------------------
def check_valid_schema(df):
    '''
    Check whether the DataFrame contains the expected columns.
    Otherwise, raise an error.
    '''

    # Define the expected columns in the source dataset.
    # These columns are required for the subsequent ETL operations.
    SCHEMA_COLS = [
        'date_time', 'id', 'type', 'subtype',
        'location', 'last_updated', 'description'
    ]

    # Compare the actual DataFrame columns with the expected schema.
    # Counter compares column names and their frequencies without depending
    # on the order in which the columns appear.
    # Raise an exception if the schema does not match.
    if Counter(df.columns) != Counter(SCHEMA_COLS):
        raise ValueError("Schema does not match with the expected schema.")


# --------------------------------------------------
# UTILITY 2: INCIDENT ID VALIDATION
# --------------------------------------------------
def check_numeric_id(df):
    '''
    Convert 'id' values to numeric.
    Non-numeric IDs are replaced with NaN.
    '''

    # Convert the incident ID column into numeric values.
    # errors='coerce' converts values that cannot be interpreted as numbers
    # into NaN instead of raising an exception.
    df['id'] = pd.to_numeric(df['id'], errors='coerce')

    # Return the DataFrame containing the converted incident IDs.
    return df


# --------------------------------------------------
# UTILITY 3: DATETIME VALIDATION
# --------------------------------------------------
def verify_datetime(df):
    '''
    Verify that date_time values follow ISO 8601 format.
    '''

    # Attempt to parse every date_time value using Python's ISO 8601 parser.
    # The row-wise apply operation checks the date_time value of each record.
    try:
        df.apply(
            lambda row: datetime.fromisoformat(row['date_time']),
            axis=1
        )

    # Catch invalid or incompatible date-time values.
    # Raise a clear error if any value cannot be parsed.
    except (ValueError, TypeError):
        raise ValueError("Invalid datetime format detected.")


# --------------------------------------------------
# UTILITY 4: MISSING VALUE VALIDATION
# --------------------------------------------------
def check_missing_values(df):
    '''
    Check missing values in required columns.
    '''

    # Define the columns that must contain values for each incident record.
    REQUIRED_COLS = ['date_time', 'id', 'type', 'location']

    # Iterate through each required column.
    for col in REQUIRED_COLS:

        # Count missing values in the current column.
        # If at least one missing value is found, raise an exception.
        if df[col].isnull().sum() > 0:
            raise ValueError(
                f"Missing values are present in the '{col}' attribute."
            )


### VALIDATION LOGIC

# --------------------------------------------------
# MAIN VALIDATION TASK
# --------------------------------------------------

# Register the validation function as a Prefect task.
@task
def validate_data(df):
    '''
    Validate schema, IDs, datetime, and required fields.
    '''

    # STEP 1: Verify that the extracted DataFrame contains
    # all expected columns and no unexpected columns.
    check_valid_schema(df)

    # STEP 2: Convert incident IDs into numeric values.
    # Non-numeric IDs are converted to NaN.
    df = check_numeric_id(df)

    # STEP 3: Verify that all date_time values can be parsed
    # using the ISO 8601 date-time format.
    verify_datetime(df)

    # STEP 4: Check whether required columns contain missing values.
    check_missing_values(df)

    # Return the validated DataFrame to the transformation stage.
    return df