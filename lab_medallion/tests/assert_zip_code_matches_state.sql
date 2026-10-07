-- Detect users whose ZIP code is outside the valid range of their state
-- The generator draws ZIP codes independently of the state, so inconsistencies are expected:
-- the test only warns to report them without blocking the pipeline
{{ config(severity='warn') }}

select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
left join {{ ref('zip_code_ranges') }} z
    on u.state = z.state_code
    and cast(u.zip_code as integer) between z.zip_min and z.zip_max
where nullif(u.zip_code, '') is not null
  and z.state_code is null
