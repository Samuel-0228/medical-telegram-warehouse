{ { config(materialized = 'table') } }
select date_key,
    full_date,
    extract(
        dow
        from full_date
    ) as day_of_week,
    to_char(full_date, 'Day') as day_name,
    extract(
        week
        from full_date
    ) as week_of_year,
    extract(
        month
        from full_date
    ) as month,
    to_char(full_date, 'Month') as month_name,
    extract(
        quarter
        from full_date
    ) as quarter,
    extract(
        year
        from full_date
    ) as year,
    case
        when extract(
            dow
            from full_date
        ) in (0, 6) then true
        else false
    end as is_weekend
from (
        select generate_series(
                (
                    select min(message_date)
                    from { { ref('stg_telegram_messages') } }
                ),
                current_date,
                interval '1 day'
            )::date as full_date
    ) dates
    cross join (
        select row_number() over () - 1 as date_key
        from generate_series(1, 10000)
    ) keys -- Placeholder keys