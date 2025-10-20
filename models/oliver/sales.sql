{{ config(
    materialized='table',
    schema='dw_oliver'
) }}

select
  dd.date_day            as sale_date,
  st.store_name          as store_name,
  st.city                as store_city,
  st.state               as store_state,
  e.employee_key         as employee_key,
  c.cust_key             as cust_key,
  e.first_name           as employeefname,
  e.last_name            as employeellname,
  c.first_name           as customerfname,
  c.last_name            as customerlname,
  p.product_key          as product_key,
  p.product_name         as product_name,
  p.description          as product_description,
  f.quantity             as quantity,
  f.unit_price           as unit_price,
  f.sales_amount         as dollars_sold
from {{ ref('fact_sales') }} f
left join {{ ref('oliver_dim_date') }}     dd on dd.date_key      = f.date_key
left join {{ ref('oliver_dim_store') }}    st on st.store_key     = f.store_key
left join {{ ref('oliver_dim_employee') }} e  on e.employee_key   = f.employee_key
left join {{ ref('oliver_dim_customer') }} c  on c.cust_key       = f.cust_key
left join {{ ref('oliver_dim_product') }}  p  on p.product_key    = f.product_key
