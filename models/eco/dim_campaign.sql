{{ config(
    materialized='table',
    schema='dw_eco'
    )
}}

with sales_campaign as (
  select
    campaign_id,
    campaign_name,
    campaign_discount
  from {{ source('eco_tx','promotional_campaign') }}
),

email_campaign as (
  select distinct
    campaignid as campaign_id,
    campaignname as campaign_name
  from {{ source('eco_landing','marketing_emails') }}
),

merged as (
  select
    coalesce(s.campaign_id, e.campaign_id) as campaign_id,
    coalesce(s.campaign_name, e.campaign_name) as campaign_name,
    s.campaign_discount
  from sales_campaign s
  full join email_campaign e
    on s.campaign_id = e.campaign_id
)

select
  {{ dbt_utils.generate_surrogate_key(['campaign_id','campaign_name']) }} as campaign_key,
  *
from merged
