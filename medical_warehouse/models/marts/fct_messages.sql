{ { config(materialized = 'table') } }
select stm.message_id,
    dc.channel_key,
    dd.date_key,
    stm.message_text,
    stm.message_length,
    stm.view_count,
    stm.forward_count,
    stm.has_image
from { { ref('stg_telegram_messages') } } stm
    left join { { ref('dim_channels') } } dc on stm.channel_name = dc.channel_name
    left join { { ref('dim_dates') } } dd on stm.message_date::date = dd.full_date