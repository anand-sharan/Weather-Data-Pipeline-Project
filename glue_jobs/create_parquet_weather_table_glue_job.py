import sys
import boto3

# AWS Resource Configuration for Weather Data Pipeline
# S3 Buckets
SOURCE_BUCKET = 'open-meteo-weather-data-parquet-bucket-04142025'
TRANSFORMED_BUCKET = 'open-meteo-weather-data-parquet-bucket-04142025'  # Same as source in this case
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

client = boto3.client('athena')

# First drop the existing table if it exists
drop_query = f"""
DROP TABLE IF EXISTS "{DATABASE_NAME}".{TRANSFORMED_TABLE_NAME}
"""

print(f"Dropping existing table if it exists: {drop_query}")

try:
    drop_query_start = client.start_query_execution(
        QueryString = drop_query,
        QueryExecutionContext = {
            'Database': DATABASE_NAME
        }, 
        ResultConfiguration = { 'OutputLocation': QUERY_RESULTS_BUCKET_URL}
    )
    
    # wait for drop query to finish
    drop_response = client.get_query_execution(QueryExecutionId=drop_query_start["QueryExecutionId"])
    resp = ["FAILED", "SUCCEEDED", "CANCELLED"]
    
    while drop_response["QueryExecution"]["Status"]["State"] not in resp:
        drop_response = client.get_query_execution(QueryExecutionId=drop_query_start["QueryExecutionId"])
    
    print(f"Drop table status: {drop_response['QueryExecution']['Status']['State']}")
except Exception as e:
    print(f"Warning: Error dropping table: {str(e)}")
    # Continue even if dropping fails

# Now create the new table
create_query = f"""
CREATE TABLE "{DATABASE_NAME}".{TRANSFORMED_TABLE_NAME} WITH
(external_location='{TRANSFORMED_BUCKET_URL}transformed_data/',
format='PARQUET',
write_compression='SNAPPY',
partitioned_by = ARRAY['yr_mo_partition'])
AS
SELECT DISTINCT
    latitude,
    longitude,
    temp AS temp_F,
    (temp - 32) * (5.0/9.0) AS temp_C,
    row_ts,
    time,
    SUBSTRING(time,1,7) AS yr_mo_partition
FROM "{DATABASE_NAME}"."{SOURCE_TABLE_NAME}"
"""

print(f"Creating new table: {create_query}")

try:
    queryStart = client.start_query_execution(
        QueryString = create_query,
        QueryExecutionContext = {
            'Database': DATABASE_NAME
        }, 
        ResultConfiguration = { 'OutputLocation': QUERY_RESULTS_BUCKET_URL}
    )

    # list of responses
    resp = ["FAILED", "SUCCEEDED", "CANCELLED"]

    # get the response
    response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])

    # wait until query finishes
    print("Waiting for query to complete...")
    while response["QueryExecution"]["Status"]["State"] not in resp:
        response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])
        
    # Check if query succeeded
    if response["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
        print(f"Successfully created table {DATABASE_NAME}.{TRANSFORMED_TABLE_NAME}")
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