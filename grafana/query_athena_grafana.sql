SELECT
   CAST(time AS TIMESTAMP) AS "time"
  ,temp_F
  ,temp_C
FROM open_meteo_weather_data_parquet_tbl_prod_2025_04_17_02_58_16_622979
WHERE $__timeFilter(CAST(time AS TIMESTAMP))
ORDER BY 1 