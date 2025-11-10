{{ config(
    materialized='table',
    schema='dw_eco'
    )
}}

select
  d.date_key as date_key,
  cu.customer_key as customer_key,
  ca.campaign_key as campaign_key,
  s.eventtype as event_type,
  1 as event_count
from {{ ref('stg_marketing_emails') }} s
join {{ ref('dim_eco_date') }} d
  on d.date_day = to_date(s.eventtimestamp)
join {{ ref('dim_eco_customer') }} cu
  on cu.first_name = s.subscriberfirstname
 and cu.last_name  = s.subscriberlastname
left join {{ ref('dim_campaign') }} ca
  on ca.campaign_id = s.campaignid
