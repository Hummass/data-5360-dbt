{{ config(materialized='table', schema='dw_oliver') }}

with
orders as (
  select
    o.order_id,
    o.order_date,
    o.store_id,
    o.customer_id,
    o.employee_id,
    o.total_amount
  from {{ source('oliver_landing','orders') }} o
),
lines as (
  select
    l.order_id,
    l.order_line_id,
    l.product_id,
    l.quantity,
    l.unit_price,
    (l.quantity * l.unit_price) as line_total
  from {{ source('oliver_landing','orderline') }} l
)

select
  st.store_key,
  emp.employee_key,
  cust.cust_key,
  dd.date_key,
  prod.product_key,

  o.order_id           as sales_order_id,
  l.order_line_id      as sales_line_id,
  l.quantity,
  l.unit_price,
  l.line_total         as sales_amount
from orders o
join lines l
  on l.order_id = o.order_id
left join {{ ref('oliver_dim_store') }}    st   on st.store_id      = o.store_id
left join {{ ref('oliver_dim_employee') }} emp  on emp.employee_id  = o.employee_id
left join {{ ref('oliver_dim_customer') }} cust on cust.customer_id = o.customer_id
left join {{ ref('oliver_dim_product') }}  prod on prod.product_id  = l.product_id
left join {{ ref('oliver_dim_date') }}     dd   on dd.date_day      = o.order_date
