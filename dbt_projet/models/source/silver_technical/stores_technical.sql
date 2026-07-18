{{ config(
    materialized='incremental',
    unique_key='store_id'
) }}

SELECT *, current_timestamp() as processed_at
FROM {{ source('supermache_databricks', 'stores') }}
{% if is_incremental() %}
  Where updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %} 