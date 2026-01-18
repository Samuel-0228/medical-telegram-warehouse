-- models/staging/stg_telegram_messages.sql
{ { config(materialized = 'table') } } with raw_messages as (
    select *
    from { { source('raw', 'telegram_messages') } }
)
select message_id,
    channel_name,
    cast(message_date as timestamp) as message_date,
    message_text,
    length(message_text) as message_length,
    cast(views as int) as view_count,
    cast(forwards as int) as forward_count,
    case
        when image_path is not null then true
        else false
    end as has_image,
    image_path
from raw_messages
where message_text is not null
    and length(message_text) > 0 -- Filter invalids