{{ config(
    materialized='table',
    schema='dw_oliver'
) 
}}

select
  c.customer_id as Cust_Key,       
  c.customer_id, 
  c.first_name,
  c.last_name,
  c.email,
  c.phone_number,
  c.state
from {{ source('oliver_landing','customer') }} c
