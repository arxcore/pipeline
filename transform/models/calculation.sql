drop table if exists final_data;
create table final_data as

with metrix as (
    select
        curr.id,
        curr.date,
        curr.year,
        curr.source,
        curr.code,
        curr.country,
        curr.category,
        curr.indicator,
        curr.value,
        curr.frequency,
        curr.method,
        curr.unit,
        curr.description,
        curr.processed,
        case
            when curr.method in ('net', 'mom') then prev_mom.value
            when curr.method = 'yoy' then prev_yoy.value
        end as prev_value,

        prev_mom.value as mom_value,
        curr.value - prev_mom.value as net_change, --net change mom
        -- mom change 
        (
            curr.value - prev_mom.value) * 100.0 / nullif(
            prev_mom.value,
            0
        ) as pct_change_mom,
        -- yoy change 
        prev_yoy.value as yoy_value,
        (
            curr.value - prev_yoy.value) * 100.0 / nullif(
            prev_yoy.value,
            0
        ) as pct_change_yoy

    from staging_data curr
    -- mom join
    left join staging_data prev_mom
        on
            curr.country = prev_mom.country
            and curr.indicator = prev_mom.indicator
            and curr.method = prev_mom.method
            and date_trunc('month', prev_mom.date)
            = date_trunc('month', curr.date - interval '1 month')
    -- yoy join 
    left join staging_data prev_yoy
        on
            curr.country = prev_yoy.country
            and curr.indicator = prev_yoy.indicator
            and curr.method = prev_yoy.method
            and date_trunc('month', prev_yoy.date)
            = date_trunc('month', curr.date - interval '1 year')
)

select
    id,
    date,
    year,
    source,
    code,
    indicator,
    country,
    category,
    value,
    prev_value,
    case
        when method = 'net' then net_change
        when method = 'mom' then pct_change_mom
        when method = 'yoy' then pct_change_yoy
        when method = 'raw' then value
    end as final_metric,
    frequency,
    method,
    unit,
    description,
    processed
from metrix
order by date desc;
