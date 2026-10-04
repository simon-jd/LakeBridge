{{ config(materialized='view', alias=var('staging_alias')) }}

select tenant_id, event_id, (event_time at time zone 'UTC')::date as business_date,
       units, billable_units
from {{ source('work', 'raw_events') }}
where job_id = '{{ var("job_id") }}'
