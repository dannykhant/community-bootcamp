with base as (select
	e.user_id,
	e.event_time::date as event_date,
	e.url,
	coalesce(d.os_type, 'N/A') as os_type,
	coalesce(d.browser_type, 'N/A') as browser_type,
	case when referrer like '%eczachly%' then 'On Site'
		when referrer like '%zachwilson%' then 'On Site'
		when referrer like '%linkedin%' then 'Linkedin'
		when referrer like '%google%' then 'Google'
		when referrer like '%t.co%' then 'Twitter'
		when referrer like '%facebook%' then 'Facebook'
		when referrer like '%youtube%' then 'Youtube'
		when referrer like '%bing%' then 'Bing'
		when referrer is null then 'Direct'
		else 'Other'
	end as referrer_mapped
from events e
join devices d
on e.device_id = d.device_id)

, agg as (select 
	event_date,
	url,
	referrer_mapped,
	count(1) as hits
from base
group by event_date, url, referrer_mapped)

, windowed as (select
	event_date,
	url,
	referrer_mapped,
	hits,
	sum(hits) over (
		partition by url, referrer_mapped, date_trunc('month', event_date)
		order by event_date
		rows between unbounded preceding and unbounded following
	) as monthly_cumulative,
	sum(hits) over (
		partition by url, referrer_mapped
		order by event_date
	) as rolling_cumulative,
	sum(hits) over (
		partition by url, referrer_mapped
		order by event_date
		rows between unbounded preceding and unbounded following
	) as total_cumulative,
	sum(hits) over (
		partition by url, referrer_mapped
		order by event_date
		rows between 6 preceding and current row
	) as weekly_rolling,
	sum (hits) over (
		partition by url, referrer_mapped
		order by event_date
		rows between 13 preceding and 6 preceding
	) as previous_weekly_rolling
from agg
order by referrer_mapped, url, event_date)

select 
	referrer_mapped,
	url,
	event_date,
	hits,
	weekly_rolling,
	previous_weekly_rolling,
	cast(hits as real) / monthly_cumulative as pct_of_month
from windowed
where total_cumulative > 500