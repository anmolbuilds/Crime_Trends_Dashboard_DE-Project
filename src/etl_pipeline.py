# Used to log pipeline execution details, errors, and progress.
import logging

# Used to calculate the total execution time of the ETL pipeline.
import time

# Import the extraction function to fetch incident data from the source.
from src.extract import extract_data

# Import the validation function to perform data quality checks.
from src.validate import validate_data

# Import the transformation function to clean and prepare the data.
from src.transform import transform_data

# Import the loading function to insert data into PostgreSQL.
from src.load import load_into_postgres

# Prefect decorator used to convert the Python function into a Prefect flow.
from prefect import flow


# Configure the logging system for the ETL pipeline.
# INFO level displays informational messages and higher-severity messages.
# The format includes the timestamp, log level, and actual message.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create a logger specific to this module.
logger = logging.getLogger(__name__)


# Define the main ETL pipeline as a Prefect flow.
# log_prints=True allows print statements to appear in Prefect logs.
@flow(name="cpd_incident_etl", log_prints=True)
def etl():
    # raise Exception("TEST ERROR: Prefect failure notification test")
    
    '''
    Execute the ETL pipeline:
    - Extract CPD incident data from the Socrata API
    - Validate and transform the extracted data to prepare it for storage
    - Import the transformed data into Postgres
    '''

    # Record the start time to calculate the total pipeline execution duration.
    start_time = time.time()

    try:
        # Log the beginning of the ETL pipeline execution.
        logger.info("Starting CPD Incident ETL pipeline")

        # --------------------------------------------------
        # STEP 1: EXTRACTION
        # --------------------------------------------------
        # Fetch incident records from the Cambridge Police Department dataset
        # through the Socrata API.
        logger.info("Extracting data...")
        extracted_df = extract_data()

        # Display the total number of records retrieved from the source.
        logger.info("Total records extracted: %d", len(extracted_df))
        print("Total records extracted:", len(extracted_df))

        # Count the number of distinct incident IDs in the extracted dataset.
        print("Unique incident IDs:", extracted_df["id"].nunique())

        # Identify how many records contain duplicate incident IDs.
        print("Duplicate ID count:", extracted_df["id"].duplicated().sum())

        # --------------------------------------------------
        # OPTIONAL DUPLICATE DIAGNOSTICS
        # --------------------------------------------------
        # The following commented-out code can be used to investigate
        # duplicate incident IDs and display sample duplicate records.

        # print("DUPLICATE DIAGNOSTIC CODE IS RUNNING")
        # duplicate_rows = extracted_df[extracted_df["id"].duplicated(keep=False)]
        # print("Sample duplicate records:")
        # print(duplicate_rows.sort_values("id").head(10).to_string(index=False))
        
        # Check whether any extracted records have missing incident IDs.
        print("Missing ID count:", extracted_df["id"].isna().sum())

        # --------------------------------------------------
        # STEP 2: VALIDATION
        # --------------------------------------------------
        # Apply data quality checks to the extracted dataset.
        # Invalid records are filtered out by the validation function.
        logger.info("Performing data quality checks...")
        validated_df = validate_data(extracted_df)

        # Log successful completion of the validation stage.
        logger.info("Validation completed successfully")

        # Display the number of records remaining after validation.
        print("Records after validation:", len(validated_df))

        # Calculate how many records were rejected during validation.
        print("Records rejected during validation:", len(extracted_df) - len(validated_df))

        # --------------------------------------------------
        # STEP 3: TRANSFORMATION
        # --------------------------------------------------
        # Apply the required transformations to the validated dataset.
        # This prepares the data for consistent storage in PostgreSQL.
        logger.info("Performing data transformations...")
        transformed_df = transform_data(validated_df)

        # Log the number of records produced after transformation.
        logger.info("Records after transformation: %d", len(transformed_df))

        # Display the number of records remaining after transformation.
        print("Records after transformation:", len(transformed_df))

        # Calculate how many records were removed during transformation.
        print("Records removed during transformation:", len(validated_df) - len(transformed_df))

        # --------------------------------------------------
        # STEP 4: LOADING
        # --------------------------------------------------
        # Load the transformed incident records into the PostgreSQL database.
        logger.info("Importing data into PostgreSQL...")
        load_into_postgres(transformed_df)

        # Log successful completion of the database loading stage.
        logger.info("PostgreSQL loading completed successfully")

        # Display the number of records submitted for database loading.
        print("Records submitted for PostgreSQL loading:", len(transformed_df))

        # --------------------------------------------------
        # PIPELINE EXECUTION SUMMARY
        # --------------------------------------------------
        # Calculate the total time taken by the ETL pipeline.
        execution_time = time.time() - start_time

        # Log successful completion and the total execution duration.
        logger.info("ETL pipeline completed successfully")
        logger.info("Total execution time: %.2f seconds", execution_time)

    # Catch any exception raised during extraction, validation,
    # transformation, or loading.
    except Exception:
        # Log the error along with its traceback for debugging.
        logger.exception("ETL pipeline failed")

        # Re-raise the exception so Prefect can recognize the flow as failed.
        raise


# Execute this block only when this Python file is run directly.
if __name__ == "__main__":

    # Register and serve the ETL flow as a Prefect deployment.
    # The deployment can be monitored and triggered through the Prefect UI.
    # The cron expression schedules the pipeline to run daily at 18:30 UTC.
    etl.serve(
        name="cpd-pipeline-deployment",
        cron="30 18 * * *"
    )