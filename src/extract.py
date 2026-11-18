# Pandas is used to convert the extracted API records into a DataFrame.
import pandas as pd

# Socrata provides a Python client to interact with the Socrata Open Data API.
from sodapy import Socrata

# Used to load environment variables from the .env file.
from dotenv import load_dotenv

# Provides access to environment variables and operating system functionality.
import os

# Prefect task decorator allows this extraction function to be monitored
# and retried automatically when an execution fails.
from prefect import task


# Define the extraction function as a Prefect task.
# retries=3 means Prefect will retry the task up to three times after failure.
# retry_delay_seconds specifies a 10-second delay between retry attempts.
@task(retries=3, retry_delay_seconds=[10, 10, 10])
def extract_data():
    '''
    Extract incident data reported to the Cambridge Police Department
    using the Socrata Open Data API.
    Return the incident data as a Pandas DataFrame.
    '''

    # Load environment variables from the .env file.
    # This keeps sensitive configuration values outside the source code.
    load_dotenv()

    # Retrieve the Socrata application token from the environment variables.
    APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

    # Check whether the application token is available.
    # Raise an error if it is missing because authentication may fail.
    if not APP_TOKEN:
        raise ValueError("SOCRATA_APP_TOKEN not found in .env file")

    # Initialize the Socrata API client.
    # The domain identifies the Cambridge Open Data portal.
    # APP_TOKEN is used for API authentication.
    # timeout=30 sets the maximum request waiting time to 30 seconds.
    client = Socrata(
        "data.cambridgema.gov",
        APP_TOKEN,
        timeout=30
    )

    # Specify the Cambridge Police Department incident dataset ID.
    DATASET_ID = "3gki-wyrb"

    # Fetch all available records from the specified dataset.
    # order="id ASC" sorts the records in ascending order of incident ID.
    results = client.get_all(DATASET_ID, order="id ASC")

    # Convert the list of records returned by the API into a Pandas DataFrame.
    results_df = pd.DataFrame.from_records(results)

    # Display the total number of records successfully fetched from the API.
    print("Total records fetched:", len(results_df))

    # Return the extracted DataFrame to the next stage of the ETL pipeline.
    return results_df