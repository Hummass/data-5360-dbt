{{ config(
    materialized='table',
    schema='dw_oliver'
    )
}}

select
  p.product_id   as Product_Key,
  p.product_id,
  p.product_name,
  p.description,
from {{ source('oliver_landing','product') }} p
