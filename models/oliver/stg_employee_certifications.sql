{{ config(
    materialized='table',
    schema='dw_oliver'
) }}

select
  employee_id,
  first_name,
  last_name,
  email,
  certification_completion_id,
  PARSE_JSON(certification_json):certification_name::string as certification_name,
  PARSE_JSON(certification_json):certification_cost::number(18,2) as certification_cost,
  PARSE_JSON(certification_json):certification_awarded_date::date as certification_awarded_date
from {{ source('oliver_landing', 'employee_certifications') }}
