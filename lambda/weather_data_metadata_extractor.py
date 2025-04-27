import json
import boto3
import logging
from datetime import datetime
import re
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    """
    Lambda function to extract metadata from weather data tables
    and save it to S3 in JSON format optimized for Athena querying
    """
    try:
        # Configurations - update these values
        source_database_name = "weather-database-04142025"
        results_bucket = "query-results-location-de-proj-04152025"
        table_name = "open_meteo_weather_data_parquet_tbl_prod_2025_04_17_02_58_16_622979"
        s3_location = f"s3://open-meteo-weather-data-parquet-bucket-04142025/{table_name}/"

        # Try to get actual table information from Glue
        try:
            table_response = glue_client.get_table(
                DatabaseName=source_database_name,
                Name=table_name
            )
            
            # Extract accurate S3 location from Glue
            if 'Table' in table_response and 'StorageDescriptor' in table_response['Table']:
                glue_s3_location = table_response['Table']['StorageDescriptor'].get('Location')
                if glue_s3_location:
                    # Make sure the S3 location ends with a slash and doesn't have double slashes
                    s3_location = glue_s3_location
                    if not s3_location.endswith('/'):
                        s3_location += '/'
                    # Replace any double slashes (except in the protocol part)
                    s3_location = re.sub(r'(?<!:)//+', '/', s3_location)
            
            logger.info(f"Using S3 location from Glue: {s3_location}")
            
        except Exception as e:
            logger.warning(f"Could not retrieve table information from Glue: {str(e)}")
        
        # Define the table information based on the provided screenshot
        table_info = {
            "table_name": table_name,
            "database_name": source_database_name,
            "columns": [
                {"name": "latitude", "type": "double", "is_partition": False},
                {"name": "longitude", "type": "double", "is_partition": False},
                {"name": "temp_f", "type": "double", "is_partition": False},
                {"name": "temp_c", "type": "double", "is_partition": False},
                {"name": "row_ts", "type": "string", "is_partition": False},
                {"name": "time", "type": "string", "is_partition": False},
                {"name": "yr_mo_partition", "type": "string", "is_partition": True}
            ],
            "partition_keys": [
                {"name": "yr_mo_partition", "type": "string"}
            ],
            "s3_location": s3_location
        }
        
        # Create metadata record
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Parse the S3 location to get the bucket name and prefix
        s3_bucket = None
        s3_prefix = None
        
        if s3_location.startswith("s3://"):
            s3_path = s3_location[5:]  # Remove "s3://"
            parts = s3_path.split("/", 1)
            if len(parts) > 0:
                s3_bucket = parts[0]
                s3_prefix = parts[1] if len(parts) > 1 else ""
                
                # Log the parsed values
                logger.info(f"Parsed S3 location - Bucket: {s3_bucket}, Prefix: {s3_prefix}")
        
        # Try to get S3 metadata if possible
        s3_objects = []
        s3_objects_count = 0
        s3_total_size_bytes = 0
        s3_latest_modification = None
        
        if s3_bucket and s3_prefix is not None:
            try:
                # Try to list objects at the root prefix
                try:
                    root_objects = get_s3_metadata(s3_bucket, s3_prefix)
                    s3_objects.extend(root_objects)
                    if root_objects:
                        logger.info(f"Found {len(root_objects)} objects at the root prefix")
                except Exception as e:
                    logger.warning(f"Error listing objects at root prefix: {str(e)}")
                
                # Check for partitions
                # Define all possible partitions to check (for the current year and previous year)
                current_year = datetime.now().year
                
                # Try a different partition format - directly under the table folder
                for partition_value in [f"{year}_{str(month).zfill(2)}" for year in range(current_year-1, current_year+1) for month in range(1, 13)]:
                    # Format 1: yr_mo_partition=YYYY_MM/
                    partition_prefix_1 = f"{s3_prefix}yr_mo_partition={partition_value}/"
                    # Format 2: YYYY_MM/ (direct partition)
                    partition_prefix_2 = f"{s3_prefix}{partition_value}/"
                    
                    for prefix in [partition_prefix_1, partition_prefix_2]:
                        try:
                            partition_objects = get_s3_metadata(s3_bucket, prefix)
                            if partition_objects:
                                logger.info(f"Found {len(partition_objects)} objects in partition prefix {prefix}")
                                s3_objects.extend(partition_objects)
                        except Exception as e:
                            logger.debug(f"No objects found in partition prefix {prefix}: {str(e)}")
            except Exception as e:
                logger.warning(f"Error in S3 metadata collection: {str(e)}")
        else:
            logger.warning(f"Could not parse S3 location: {s3_location}")
        
        # Update metadata with S3 information
        if s3_objects:
            s3_objects_count = len(s3_objects)
            s3_total_size_bytes = sum(obj.get("size", 0) for obj in s3_objects)
            latest_mod_objects = [obj for obj in s3_objects if obj.get("last_modified")]
            if latest_mod_objects:
                s3_latest_modification = max(obj.get("last_modified") for obj in latest_mod_objects)
                if isinstance(s3_latest_modification, datetime):
                    s3_latest_modification = s3_latest_modification.isoformat()
                
            logger.info(f"Updated metadata with {len(s3_objects)} objects")
        else:
            logger.warning("No S3 objects found")
        
        # Create a flattened metadata record for better Athena compatibility
        # Note: We're converting complex nested structures to strings
        flattened_metadata = {
            "table_name": table_name,
            "source_database": source_database_name,
            "extraction_time": current_time,
            "column_count": len(table_info["columns"]),
            "s3_location": s3_location,
            "s3_objects_count": s3_objects_count,
            "s3_total_size_bytes": s3_total_size_bytes,
            "s3_latest_modification": s3_latest_modification
        }
        
        # Save flattened metadata to S3
        metadata_str = json.dumps(flattened_metadata)
        current_date = datetime.now().strftime("%Y-%m-%d")
        metadata_folder = f"weather_metadata_flat/{table_name}/date={current_date}/"
        metadata_file_key = f"{metadata_folder}{current_time.replace(' ', '_').replace(':', '-')}.json"
        
        try:
            # Create a subfolder in results bucket
            s3_client.put_object(
                Bucket=results_bucket,
                Key=metadata_folder,
                Body=''
            )
            
            # Save metadata file
            s3_client.put_object(
                Bucket=results_bucket,
                Key=metadata_file_key,
                Body=metadata_str,
                ContentType='application/json'
            )
            logger.info(f"Saved flattened metadata to S3: s3://{results_bucket}/{metadata_file_key}")
            
            # Create a manifest file in a separate folder
            manifest_content = json.dumps({
                "latest_metadata": f"s3://{results_bucket}/{metadata_file_key}",
                "last_updated": current_time,
                "table_name": table_name
            })
            
            s3_client.put_object(
                Bucket=results_bucket,
                Key=f"weather_metadata_manifests/{table_name}/manifest.json",
                Body=manifest_content,
                ContentType='application/json'
            )
            
            # Save the detailed metadata separately (optional)
            # This version contains the full nested structure
            detailed_metadata = {
                "table_name": table_name,
                "source_database": source_database_name,
                "extraction_time": current_time,
                "column_count": len(table_info["columns"]),
                "columns": table_info["columns"],
                "partition_keys": table_info["partition_keys"],
                "s3_location": s3_location,
                "s3_objects_count": s3_objects_count,
                "s3_total_size_bytes": s3_total_size_bytes,
                "s3_latest_modification": s3_latest_modification,
                "s3_objects_sample": s3_objects[:5] if s3_objects else []
            }
            
            detailed_file_key = f"weather_metadata_detailed/{table_name}/{current_time.replace(' ', '_').replace(':', '-')}.json"
            
            s3_client.put_object(
                Bucket=results_bucket,
                Key=detailed_file_key,
                Body=json.dumps(detailed_metadata, default=str),
                ContentType='application/json'
            )
            
            logger.info(f"Saved detailed metadata to S3: s3://{results_bucket}/{detailed_file_key}")
            
        except Exception as e:
            logger.error(f"Could not save metadata to S3: {str(e)}")
            raise
        
        # Construct response
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Successfully saved weather data metadata",
                "flattened_metadata_location": f"s3://{results_bucket}/{metadata_file_key}",
                "detailed_metadata_location": f"s3://{results_bucket}/{detailed_file_key}",
                "manifest_location": f"s3://{results_bucket}/weather_metadata_manifests/{table_name}/manifest.json",
                "metadata_summary": {
                    "objects_count": s3_objects_count,
                    "total_size_bytes": s3_total_size_bytes
                },
                "crawler_instructions": """
To create a table from the flattened metadata, run a Glue crawler with:
Target path: s3://{results_bucket}/weather_metadata_flat/
Exclude patterns: **/manifest.json
                """.format(results_bucket=results_bucket)
            })
        }
    except Exception as e:
        logger.error(f"Error processing weather data metadata: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": f"Error: {str(e)}"
            })
        }

def get_s3_metadata(bucket_name, prefix):
    """
    Retrieve metadata for objects in the S3 bucket
    """
    try:
        s3_objects = []
        paginator = s3_client.get_paginator('list_objects_v2')
        
        # Clean up the prefix - remove any leading slashes
        if prefix.startswith('/'):
            prefix = prefix[1:]
        
        # Log the exact bucket and prefix we're using
        logger.debug(f"Listing objects in bucket '{bucket_name}' with prefix '{prefix}'")
        
        page_iterator = paginator.paginate(
            Bucket=bucket_name, 
            Prefix=prefix,
            PaginationConfig={'MaxItems': 10000}
        )
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    s3_objects.append({
                        "key": obj.get('Key'),
                        "size": obj.get('Size'),
                        "last_modified": obj.get('LastModified'),
                        "storage_class": obj.get('StorageClass'),
                        "etag": obj.get('ETag')
                    })
        
        logger.info(f"Retrieved {len(s3_objects)} objects from s3://{bucket_name}/{prefix}")
        return s3_objects
    except Exception as e:
        logger.debug(f"Error retrieving S3 metadata for s3://{bucket_name}/{prefix}: {str(e)}")
        return []