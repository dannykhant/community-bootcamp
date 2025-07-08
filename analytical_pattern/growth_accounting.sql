-- ddl
create table users_growth_accounting (
	user_id numeric,
	first_active_date date,
	last_active_date date,
	daily_active_state text,
	weekly_active_state text,
	dates_active date[],
	date date,
	primary key(user_id, date)
)

insert into users_growth_accounting
with yesterday as (
	select *
	from users_growth_accounting
	where date = date('2023-01-16')
)
, today as (
	select 
		user_id,
		date(event_time) as today_date
	from events
	where date(event_time) = date('2023-01-17')
		and user_id is not null
	group by user_id, date(event_time)
)
select 
	coalesce(t.user_id, y.user_id) as user_id,
	coalesce(y.first_active_date, t.today_date) as first_active_date,
	coalesce(t.today_date, y.last_active_date) as last_active_date,
	case when y.user_id is null
			then 'New'
		when y.last_active_date = t.today_date - interval '1 day'
			then 'Retained'
		when y.last_active_date < t.today_date - interval '1 day'
			then 'Resurrected'
		when t.today_date is null and y.last_active_date = y.date
			then 'Churned'
		else 'Stale'
	end as daily_active_state,
	case when y.user_id is null
			then 'New'
		when y.last_active_date > coalesce(t.today_date, y.date + interval '1 day') - interval '7 day'
			then 'Retained'
		when y.last_active_date < t.today_date - interval '7 day'
			then 'Resurrected'
		when t.today_date is null 
			and y.last_active_date = (y.date - interval '6 day')
			then 'Churned'
		else 'Stale'
	end as weekly_active_state,
	case when y.dates_active is null then array[t.today_date]
		when t.today_date is not null
			then y.dates_active || array[t.today_date]
		else y.dates_active
	end as dates_active,
	coalesce(t.today_date, y.date + interval '1 day')::date as date
from today t
full outer join yesterday y
on t.user_id = y.user_id

-- retention analysis
select 
	date - first_active_date as days_since_first_active,
	count(case when daily_active_state in ('Retained', 'Resurrected', 'New')
			then 1 end) as num_active,
	count(1) as total,
	cast(count(case when daily_active_state in ('Retained', 'Resurrected', 'New')
			then 1 end) as real) / count(1) as pct_active
from users_growth_accounting
group by date - first_active_date
order by 1
