# Pandas is used for data manipulation and transformation operations.
import pandas as pd

# Prefect task decorator allows the transformation function
# to be monitored as part of the ETL workflow.
from prefect import task


### UTILITIES

# --------------------------------------------------
# UTILITY 1: REMOVE DUPLICATE RECORDS
# --------------------------------------------------
def remove_duplicates(df):
    '''
    Remove duplicate rows based on 'id'. Keep the first occurrence.
    '''

    # Store the number of records before removing duplicates.
    before = len(df)

    # Identify duplicate records using the incident ID column.
    # Keep the first occurrence of each ID and remove subsequent occurrences.
    df = df.drop_duplicates(subset=["id"], keep='first')

    # Calculate the number of records removed during deduplication.
    removed = before - len(df)

    # Display the number of duplicate records removed.
    print(f"Duplicate records removed: {removed}")

    # Return the DataFrame containing unique incident IDs.
    return df


# --------------------------------------------------
# UTILITY 2: REMOVE INVALID RECORDS
# --------------------------------------------------
def remove_invalid_rows(df):
    '''
    Remove rows where the 'id' is NaN.
    '''

    # Store the number of records before removing invalid rows.
    before = len(df)

    # Remove records where the incident ID is missing (NaN).
    # A valid incident ID is required to identify each record.
    df = df.dropna(subset='id')

    # Calculate the number of records removed due to missing IDs.
    removed = before - len(df)

    # Display the number of records removed.
    print(f"Records removed due to missing IDs: {removed}")

    # Return the DataFrame containing records with valid incident IDs.
    return df


# --------------------------------------------------
# UTILITY 3: DATETIME FEATURE EXTRACTION
# --------------------------------------------------
def split_datetime(df):
    '''
    Split date_time into separate year, month, day, and time columns.
    '''

    # Convert the date_time column into Pandas datetime objects.
    # This enables extraction of individual date and time components.
    # Convert to datetime
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Extract individual temporal features from the date_time column.
    # These additional columns can be used for time-based analysis
    # and visualizations in the dashboard.

    # Extract the year from each incident timestamp.
    # Extract year/month/day/time
    df['year'] = df['date_time'].dt.year

    # Extract the full month name (January, February, etc.).
    df['month'] = df['date_time'].dt.month_name()

    # Extract the day of the week (Monday, Tuesday, etc.).
    df['day'] = df['date_time'].dt.day_name()

    # Extract the hour component (0–23).
    df['hour'] = df['date_time'].dt.hour

    # Extract the minute component (0–59).
    df['minute'] = df['date_time'].dt.minute

    # Extract the second component (0–59).
    df['second'] = df['date_time'].dt.second

    # Return the DataFrame with the newly extracted datetime features.
    return df


### TRANSFORMATION LOGIC

# --------------------------------------------------
# MAIN TRANSFORMATION TASK
# --------------------------------------------------

# Register the transformation function as a Prefect task.
@task
def transform_data(df):
    '''
    Apply transformations:
    - Remove duplicate records
    - Remove invalid rows
    - Split datetime into separate columns
    '''

    # Store the original number of records before any transformations.
    # This will be used to calculate the total number of records removed.
    original_count = len(df)

    # STEP 1: Remove duplicate incident records.
    # Only the first record for each incident ID is retained.
    df = remove_duplicates(df)

    # STEP 2: Remove records with missing incident IDs.
    # This ensures that every retained record has a valid identifier.
    df = remove_invalid_rows(df)

    # Display the total number of records removed across both operations.
    print(f"Total records removed during transformation: {original_count - len(df)}")

    # Display the total number of records remaining after cleaning.
    print(f"Total records after transformation: {len(df)}")

    # STEP 3: Extract separate temporal features from the date_time column.
    # These features support year-wise, monthly, daily, and hourly analysis.
    df = split_datetime(df)

    # Return the transformed DataFrame to the loading stage.
    return df