# Used to safely encode special characters in database credentials
# when constructing the PostgreSQL connection URL.
from urllib.parse import quote_plus

# Prefect task decorator allows the loading operation to be monitored
# as part of the ETL workflow.
from prefect import task

# SQLAlchemy is used to create database connections and execute SQL queries.
from sqlalchemy import create_engine, text

# psycopg2 provides a PostgreSQL database adapter for Python.
# It is used here to establish a direct database connection.
import psycopg2

# Used to load database credentials and other configuration values
# from the .env file.
from dotenv import load_dotenv

# Provides access to environment variables.
import os


# Load environment variables from the .env file.
# This file contains PostgreSQL credentials, keeping sensitive information
# separate from the application source code.
# read content from .env, which contains our Postgres credentials
load_dotenv()


# --------------------------------------------------
# UTILITY 1: CREATE POSTGRESQL TABLE
# --------------------------------------------------

def create_postgres_table():
    '''
    Create the cpd_incidents table in Postgres DB (cpd_db) if it doesn't exist.
    '''

    # Establish a direct connection to the PostgreSQL database.
    # The connection details are obtained from the local PostgreSQL setup
    # and the credentials stored in environment variables.
    # establish connection to DB
    conn = psycopg2.connect(
        host="localhost",
        port="5433",
        database="cpd_db",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    # Create a cursor object.
    # The cursor is used to execute SQL statements against PostgreSQL.
    # create cursor object to execute SQL
    cur = conn.cursor()

    # Define the SQL query to create the target table.
    # CREATE TABLE IF NOT EXISTS ensures that an existing table
    # is not recreated or overwritten.
    # The incident ID is defined as the PRIMARY KEY to uniquely
    # identify each incident record.
    # create query to create the table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS cpd_incidents (
            date_time TIMESTAMP,
            id INTEGER PRIMARY KEY,
            type TEXT,
            subtype TEXT,
            location TEXT,
            description TEXT,
            last_updated TIMESTAMP,
            year INTEGER,
            month TEXT,
            day TEXT,
            hour INTEGER,
            minute INTEGER,
            second INTEGER
        )
    '''

    # Execute the table creation query in PostgreSQL.
    cur.execute(create_table_query)

    # Commit the transaction to permanently save the table creation.
    # commit changes
    conn.commit()

    # Close the cursor and database connection to release resources.
    # close cursor and connection
    cur.close()
    conn.close()


# --------------------------------------------------
# MAIN LOADING TASK
# --------------------------------------------------

# Register the loading function as a Prefect task.
@task
def load_into_postgres(df):
    '''
    Loads the transformed data passed in as a DataFrame
    into the 'cpd_incidents' table in our Postgres instance.
    Uses UPSERT to insert new records and update existing records.
    '''

    # STEP 1: Ensure that the target PostgreSQL table exists.
    # If the table does not exist, create_postgres_table() creates it.
    # create table to insert data into as necessary
    create_postgres_table()

    # STEP 2: Create a SQLAlchemy Engine.
    # The engine manages database connections and is used to transfer
    # the Pandas DataFrame and execute SQL queries.
    # quote_plus() safely encodes special characters in the credentials.
    # create Engine object to connect to DB
    engine = create_engine(
        f"postgresql://{quote_plus(os.getenv('POSTGRES_USER'))}:{quote_plus(os.getenv('POSTGRES_PASSWORD'))}@localhost:5433/cpd_db"
    )

    # Define the name of the staging table.
    # Incoming records are first loaded into this table before
    # being inserted or updated in the main target table.
    staging_table = "cpd_incidents_staging"

    # STEP 3: Load the transformed Pandas DataFrame into PostgreSQL.
    # to_sql() transfers the DataFrame records into the staging table.
    # if_exists='replace' replaces the staging table if it already exists.
    # index=False prevents the Pandas DataFrame index from being stored
    # as an additional database column.
    # load data into a temporary staging table
    df.to_sql(
        staging_table,
        engine,
        if_exists='replace',
        index=False
    )

    # STEP 4: Define the UPSERT SQL query.
    # UPSERT combines INSERT and UPDATE operations.
    # New incident IDs are inserted into the target table.
    # If an incident ID already exists, its corresponding record is updated.
    # insert new records and update existing records
    upsert_query = text('''
        INSERT INTO cpd_incidents (
            date_time, id, type, subtype, location, description,
            last_updated, year, month, day, hour, minute, second
        )
        SELECT
            date_time, id, type, subtype, location, description,
            last_updated, year, month, day, hour, minute, second
        FROM cpd_incidents_staging
        ON CONFLICT (id)
        DO UPDATE SET
            date_time = EXCLUDED.date_time,
            type = EXCLUDED.type,
            subtype = EXCLUDED.subtype,
            location = EXCLUDED.location,
            description = EXCLUDED.description,
            last_updated = EXCLUDED.last_updated,
            year = EXCLUDED.year,
            month = EXCLUDED.month,
            day = EXCLUDED.day,
            hour = EXCLUDED.hour,
            minute = EXCLUDED.minute,
            second = EXCLUDED.second
    ''')

    # STEP 5: Execute the UPSERT operation inside a database transaction.
    # engine.begin() manages the transaction and automatically commits
    # it when the block completes successfully.
    with engine.begin() as connection:

        # Execute the UPSERT query to insert new records
        # and update records with existing incident IDs.
        connection.execute(upsert_query)

        # STEP 6: Remove the staging table after the UPSERT operation.
        # The staging table is no longer required once the records
        # have been transferred to the main target table.
        connection.execute(
            text(f"DROP TABLE IF EXISTS {staging_table}")
        )

    # STEP 7: Dispose of the SQLAlchemy engine.
    # This releases the database connection resources.
    engine.dispose()

    # Display a confirmation message after successful completion.
    print("PostgreSQL UPSERT completed successfully.")