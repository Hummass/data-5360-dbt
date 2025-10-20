{{ config(
    materialized='table', 
    schema='dw_oliver'
    ) 
}}

select
  e.employee_id  as Employee_Key,
  e.employee_id,       
  e.first_name,
  e.last_name,
  e.email,
  e.phone_number,
  e.hire_date,
  e.position
from {{ source('oliver_landing','employee') }} e