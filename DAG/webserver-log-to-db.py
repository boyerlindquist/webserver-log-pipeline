from datetime import timedelta
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from dotenv import load_dotenv
import requests
import psycopg2

# Define the path for the input and output files
input_file = '/home/imam/data_engineer/web-server-access-log.txt'
extracted_file = '/home/imam/data_engineer/extracted-webserver-log.txt'
transformed_file = '/home/imam/data_engineer/transformed-webserver-log.csv'

load_dotenv(dotenv_path='/home/imam/data_engineer/config.env')

# PostgreSQL connection config
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_DATABASE"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

# 1. Download the Data
def download_file():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(input_file, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"File downloaded successfully: {input_file}")

# 2. Extracting the Data
def extract():
    with open(input_file, 'r') as infile, open(extracted_file, 'w') as outfile:
        next(infile) # skip the header
        for line in infile:
            fields = line.split('#')
            outfile.write('#'.join(fields[:5]) + '\n')

# 3. Transforming the Data
def transform():
    with open(extracted_file, 'r') as infile, open(transformed_file, 'w') as outfile:
        for line in infile:
            capitalized_line = line.upper()
            processed_line = capitalized_line.replace('#', ',')
            outfile.write(processed_line)

# 4. Loading the Data into PostgreSQL
def load():
    print('Loading data into PostgreSQL')

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS censor_log (
            timestamp TIMESTAMP,
            latitude VARCHAR(50),
            longitude VARCHAR(50),
            visitorid VARCHAR(50),
            accessed_from_mobile VARCHAR(50),
            execution_time TIMESTAMP
        );
    """)

    # Truncate table to ensure next run starts with clean state and preventing duplication from previous execution
    cursor.execute("TRUNCATE TABLE censor_log;")
    execution_time = datetime.now()

    # Load the transformed data into PostgreSQL
    with open(transformed_file, 'r') as infile:
        for line in infile:
            timestamp, latitude, longitude, visitorid, mobile = line.strip().split(',')
            cursor.execute(
                """
                INSERT INTO censor_log 
                (timestamp, latitude, longitude, visitorid, accessed_from_mobile, execution_time)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (timestamp, latitude, longitude, visitorid, mobile, execution_time)
            )

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    print('Data loaded successfully into PostgreSQL')

# Defining default arguments for the DAG
default_args = {
    'owner': 'Imam',
    'start_date': days_ago(0),
    'email': ['imammamqs@gmail.com'],
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'ETL_server_log_to_db',
    default_args=default_args,
    description='ETL of censor log to local postgres database',
    schedule_interval=timedelta(days=1)
)

# Define the tasks
download_task = PythonOperator(
    task_id='download_file',
    python_callable=download_file,
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

# Set task dependencies
download_task >> extract_task >> transform_task >> load_task
