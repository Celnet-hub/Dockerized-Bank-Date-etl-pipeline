{{ config(materialized='view') }}

-- Staging model for largest banks data
-- This model cleans and standardizes the raw data from the extraction process

SELECT 
    "Banks" as bank_name,
    -- Clean the assets column by removing any non-numeric characters and converting to numeric. Data was loaded as a text (VARCHAR)
    CAST(
        REGEXP_REPLACE("Assets_Billions_USD_2025", '[^0-9.]', '', 'g') AS DECIMAL(10,2)
    ) as assets_usd_billions,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('public', 'largest_banks_2025') }}
WHERE "Banks" IS NOT NULL 
  AND "Assets_Billions_USD_2025" IS NOT NULL