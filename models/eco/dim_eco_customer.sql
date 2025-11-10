{{ config(
    materialized='table',
    schema='dw_eco'
    ) 
}}

with sales_src as (
  select distinct
    customer_id,
    customer_first_name as first_name,
    customer_last_name as last_name,
    null as email,
    customer_phone as phone,
    customer_address as address,
    customer_city as city,
    customer_state as state,
    customer_zip as zipcode,
    customer_country as country
  from {{ source('eco_tx','customer') }}
),

email_src as (
  select distinct
    subscriberfirstname as first_name,
    subscriberlastname as last_name,
    subscriberemail as email
  from {{ source('eco_landing','marketing_emails') }}
),

merged as (
  select
    s.customer_id,
    coalesce(s.first_name, e.first_name) as first_name,
    coalesce(s.last_name, e.last_name) as last_name,
    coalesce(s.email, e.email) as email,
    s.phone, s.address, s.city, s.state, s.zipcode, s.country
  from sales_src s
  full join email_src e
    on s.first_name = e.first_name
   and s.last_name = e.last_name
)

select
  {{ dbt_utils.generate_surrogate_key(['first_name','last_name']) }} as customer_key,
  *
from merged
