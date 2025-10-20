{{ config(
    materialized='table',
    schema='dw_oliver'
    ) 
}}

select
  s.store_id   as Store_Key,
  s.store_id,
  s.store_name,
  s.street,
  s.city,
  s.state
from {{ source('oliver_landing','store') }} s
