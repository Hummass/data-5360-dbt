{{ config(
    materialized='table',
    schema='dw_eco'
    )
}}

with o as (
  select
    order_id,
    customer_id,
    order_timestamp
  from {{ source('eco_tx','order_tbl') }}
),

ol as (
  select
    order_line_id,
    order_id,
    product_id,
    campaign_id,
    quantity,
    discount,
    price_after_discount,
    nullif(quantity,0) as qty_nonzero
  from {{ source('eco_tx','order_line') }}
)

select
  d.date_key as date_key,
  cu.customer_key as customer_key,
  p.product_key as product_key,
  ca.campaign_key as campaign_key,
  ol.quantity as quantity,
  coalesce(ol.price_after_discount / ol.qty_nonzero, p.price) as unit_price,
  ol.price_after_discount as total_sales,
  o.order_id,
  ol.order_line_id
from ol
join o
  on o.order_id = ol.order_id
join {{ ref('dim_eco_date') }} d  on d.date_day = to_date(o.order_timestamp)
join {{ ref('dim_eco_customer') }} cu on cu.customer_id = o.customer_id
join {{ ref('dim_eco_product') }} p  on p.product_id = ol.product_id
left join {{ ref('dim_campaign') }} ca on ca.campaign_id = ol.campaign_id
