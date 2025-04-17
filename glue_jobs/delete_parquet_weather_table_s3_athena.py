import sys
import json
import boto3

# AWS Resource Configuration for Weather Data Pipeline
# S3 Buckets
SOURCE_BUCKET = 'open-meteo-weather-data-parquet-bucket-04142025'
TRANSFORMED_BUCKET = 'open-meteo-weather-data-parquet-bucket-04142025'  # Same bucket as source in this case
PROD_BUCKET = 'parquet-weather-table-prod-04142025'
QUERY_RESULTS_BUCKET = 'query-results-location-de-proj-04152025'

# Full S3 URLs
SOURCE_BUCKET_URL = f's3://{SOURCE_BUCKET}/'
TRANSFORMED_BUCKET_URL = f's3://{TRANSFORMED_BUCKET}/'
PROD_BUCKET_URL = f's3://{PROD_BUCKET}/'
QUERY_RESULTS_BUCKET_URL = f's3://{QUERY_RESULTS_BUCKET}/'

# Database
DATABASE_NAME = 'weather-database-04142025'

# Tables
SOURCE_TABLE_NAME = 'weather_open_meteo_weather_data_parquet_bucket_04142025'
TRANSFORMED_TABLE_NAME = 'open_meteo_weather_data_parquet_tbl'
PROD_TABLE_NAME = 'open_meteo_weather_data_parquet_tbl_PROD'

# For delete_parquet_weather_table_s3_athena.py
BUCKET_TO_DEL = None  # DON'T DELETE ANY S3 OBJECTS, JUST DROP THE TABLE
DATABASE_TO_DEL = DATABASE_NAME
TABLE_TO_DEL = TRANSFORMED_TABLE_NAME
QUERY_OUTPUT_BUCKET = QUERY_RESULTS_BUCKET_URL

print("Running delete script - DROPPING TABLE ONLY, NOT DELETING S3 OBJECTS")

# drop the table ONLY - do NOT delete objects from S3
client = boto3.client('athena')

# Try a simpler approach without quoting the database name
queryString = f"DROP TABLE IF EXISTS {TABLE_TO_DEL}"

print(f"Executing query: {queryString}")

try:
    queryStart = client.start_query_execution(
        QueryString = queryString,
        QueryExecutionContext = {
            'Database': DATABASE_TO_DEL
        }, 
        ResultConfiguration = { 'OutputLocation': QUERY_OUTPUT_BUCKET}
    )

    # list of responses
    resp = ["FAILED", "SUCCEEDED", "CANCELLED"]

    # get the response
    response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])

    # wait until query finishes
    while response["QueryExecution"]["Status"]["State"] not in resp:
        response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])
        
    # Check if query succeeded
    if response["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
        print(f"Successfully dropped table {TABLE_TO_DEL} from database {DATABASE_TO_DEL}")
    # if it fails, exit and give the Athena error message in the logs
    elif response["QueryExecution"]["Status"]["State"] == "FAILED":
        error_message = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
        print(f"Query failed: {error_message}")
        sys.exit(error_message)
    else:
        print(f"Query {response['QueryExecution']['Status']['State']}")
        
except Exception as e:
    error_message = f"Error executing Athena query: {str(e)}"
    print(error_message)
    sys.exit(error_message)