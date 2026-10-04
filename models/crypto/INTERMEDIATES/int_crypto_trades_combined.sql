with historical_source as (
    select
        -- Primary Identifiers
        trade_id as event_id,
        null as stream_offset,
        
        -- Core Attributes
        'bitcoin' AS crypto_name,
        price_usd,
        trade_quantity,
        total_usd_value,
        
        -- Timestamps
        trade_timestamp as event_timestamp,
        
        -- Historical Trade Specific Metadata
        is_buyer_maker,
        is_best_match,
        
        -- Raw Data & Source Lineage
        null as raw_trade_json,
        'HISTORICAL' as data_source
    from {{ ref('stg_historical_crypto') }}
),

streaming_source as (
    select
        -- Primary Identifiers
        sequence_number as event_id,
        stream_offset,
        
        -- Core Attributes
        crypto_name,
        price_usd,
        null as trade_quantity,       
        null as total_usd_value,   
        
        -- Timestamps
        enqueued_timestamp as event_timestamp,
        
        -- Historical Specifics (Not available in stream)
        null as is_buyer_maker,
        null as is_best_match,
        
        -- Raw Data & Source Lineage
        raw_trade_json,
        'STREAMING' as data_source
    from {{ ref('stg_streaming_crypto_data') }}
),

unify_all_columns as (
    select * from historical_source
    union all
    select * from streaming_source
),

deduplicated_trades as (
    select
        *,
        row_number() over (
            partition by crypto_name, price_usd,
            date_trunc('second', event_timestamp)
            order by event_timestamp desc
        ) as row_num
    from unify_all_columns
)

select
    event_id,
    stream_offset,
    crypto_name,
    {{ round_price('price_usd', 2) }} as price_usd,
    trade_quantity,
    total_usd_value,
    event_timestamp,
    is_buyer_maker,
    is_best_match,
    raw_trade_json,
    data_source
from deduplicated_trades
where row_num = 1