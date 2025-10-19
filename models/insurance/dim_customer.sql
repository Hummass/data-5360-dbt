{{ config(
    materialized = 'table', 
    schema = 'dw_insurance'
)
}}

select 
    customerid as customer_key,
    customerid,
    firstname,
    lastname,
    dob,
    address,
    city,
    state,
    zipcode
From {{source('insurance_landing', 'customers')}}