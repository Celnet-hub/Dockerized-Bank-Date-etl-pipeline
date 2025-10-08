{{ config(materialized='table') }}

-- Final transformation model that converts USD to multiple currencies
-- This creates the final fact table with currency conversions

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_largest_banks') }}
),

exchange_rates AS (
    SELECT * FROM {{ ref('exchange_rates') }}
),

currency_conversions AS (
    SELECT 
        s.bank_name,
        s.assets_usd_billions,
        
        -- Convert USD to other currencies
        ROUND(CAST(s.assets_usd_billions * gbp.exchange_rate_to_usd AS NUMERIC), 2) as assets_gbp_billions,
        ROUND(CAST(s.assets_usd_billions * eur.exchange_rate_to_usd AS NUMERIC), 2) as assets_eur_billions,
        ROUND(CAST(s.assets_usd_billions * inr.exchange_rate_to_usd AS NUMERIC), 2) as assets_inr_billions,
        
        s.loaded_at,
        CURRENT_TIMESTAMP as transformed_at
        
    FROM staging_data s
    CROSS JOIN (SELECT exchange_rate_to_usd FROM exchange_rates WHERE currency = 'GBP') gbp
    CROSS JOIN (SELECT exchange_rate_to_usd FROM exchange_rates WHERE currency = 'EUR') eur  
    CROSS JOIN (SELECT exchange_rate_to_usd FROM exchange_rates WHERE currency = 'INR') inr
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY assets_usd_billions DESC) as rank,
    bank_name,
    assets_usd_billions,
    assets_gbp_billions,
    assets_eur_billions,
    assets_inr_billions,
    loaded_at,
    transformed_at
FROM currency_conversions
ORDER BY assets_usd_billions DESC