{{ config(materialized='table', alias=var('result_alias')) }}

select tenant_id, business_date, count(*)::bigint as event_count,
       sum(units)::numeric(28,6) as total_units,
       sum(billable_units)::numeric(28,6) as billable_units
from {{ ref('stg_usage') }}
group by tenant_id, business_date
