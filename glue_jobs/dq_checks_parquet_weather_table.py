import sys
import awswrangler as wr

# AWS Resource Configuration for Weather Data Pipeline
# Database
DATABASE_NAME = 'weather-database-04142025'

# Tables
TRANSFORMED_TABLE_NAME = 'open_meteo_weather_data_parquet_tbl'

# This check counts the number of NULL rows in the temp_C column
# If any rows are NULL, the check returns a number > 0
NULL_DQ_CHECK = f"""
SELECT 
    SUM(CASE WHEN temp_C IS NULL THEN 1 ELSE 0 END) AS res_col
FROM "{DATABASE_NAME}"."{TRANSFORMED_TABLE_NAME}"
;
"""

print(f"Running data quality check on {DATABASE_NAME}.{TRANSFORMED_TABLE_NAME}")

# Run the quality check using the SAME database as the other scripts
df = wr.athena.read_sql_query(sql=NULL_DQ_CHECK, database=DATABASE_NAME)

# Exit if we get a result > 0
# Else, the check was successful
if df['res_col'][0] > 0:
    error_message = f"Results returned. Quality check failed. Found {df['res_col'][0]} NULL values in temp_C column."
    print(error_message)
    sys.exit(error_message)
else:
    print('Quality check passed. No NULL values found in temp_C column.')