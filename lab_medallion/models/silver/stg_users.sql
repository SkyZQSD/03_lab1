with source as (
    select * from {{ source('bronze', 'users') }}
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        lower(trim(username)) as username_normalized,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate_raw,
        -- The address spans 2 lines: the street, then the city, the state and the zip code
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
),

first_orders as (
    select
        user_id,
        min(ordered_at) as first_ordered_at
    from {{ ref('stg_orders') }}
    group by user_id
),

validated as (
    select
        typed.*,
        -- A birthdate is valid when it exists and is not later than the first order
        coalesce(
            typed.birthdate_raw is not null
            and (first_orders.first_ordered_at is null or typed.birthdate_raw <= first_orders.first_ordered_at),
            false
        ) as birthdate_is_valid
    from typed
    left join first_orders on typed.user_id = first_orders.user_id
)

select
    user_id,
    username,
    username_normalized,
    name,
    sex,
    email,
    case when birthdate_is_valid then birthdate_raw end as birthdate,
    birthdate_is_valid,
    street,
    -- Military addresses, such as "DPO AE 12345", have no comma
    nullif(regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from validated
