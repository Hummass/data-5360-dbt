{{ config(
    materialized='table',
    schema='dw_eco'
    )
}}

select
  emaileventid,
  emailid,
  emailname,
  campaignid,
  campaignname,
  customerid,
  subscriberid,
  subscriberemail,
  subscriberfirstname,
  subscriberlastname,
  eventtype,
  eventtimestamp,
  sendtimestamp
from {{ source('eco_landing','marketing_emails') }}
