-- ddl
-- create table array_metrics (
-- 	user_id numeric,
-- 	month_start date,
-- 	metric_name text,
-- 	metric_array real[],
-- 	primary key(user_id, month_start, metric_name)
-- )


insert into array_metrics
with daily_aggregate as (
	select 
		user_id,
		date(event_time) as date,
		count(1) as num_site_hits
	from events
	where date(event_time) = date('2023-01-05')
		and user_id is not null
	group by user_id, date(event_time)
)
, yesterday as (
	select
		*
	from array_metrics
	where month_start = date('2023-01-01')
)
select 
	coalesce(da.user_id, y.user_id) as user_id,
	coalesce(y.month_start, date_trunc('month', da.date)) as month_start,
	'site_hits' as metric_name,
	case when y.metric_array is null 
			then array_fill(0, array[date - date_trunc('month', da.date)::date]) || array[da.num_site_hits]
		when da.date is not null
			then y.metric_array || array[da.num_site_hits]
		else y.metric_array || array[0]
	end as metric_array
from daily_aggregate da
full outer join yesterday y
on da.user_id = y.user_id
on conflict(user_id, month_start, metric_name)
do
	update set metric_array = excluded.metric_array