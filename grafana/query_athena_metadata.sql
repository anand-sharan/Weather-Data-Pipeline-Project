SELECT
  extraction_time,
  table_name,
  column_count,
  s3_objects_count,
  CAST(s3_total_size_bytes AS DOUBLE) / 1024 / 1024 AS total_size_mb,
  source_database
FROM
  weather_metadata_results.weather_metadata_flat
WHERE
  table_name LIKE '%open_meteo_weather_data_parquet_tbl_prod%'
ORDER BY
  extraction_time DESC