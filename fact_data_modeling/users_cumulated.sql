-- -- ddl
-- create table users_cumulated (
-- 	user_id numeric,
-- 	dates_active date[],
-- 	date date,
-- 	primary key (user_id, date)
-- )

insert into users_cumulated
with yesterday as (
	select *
	from users_cumulated
	where date = date('2023-01-30')
)
, today as (
	select 
		user_id,
		date(event_time) as date_active
	from events
	where date(event_time) = date('2023-01-31')
		and user_id is not null
	group by user_id, date(event_time)
)

select
	coalesce(t.user_id, y.user_id) as user_id,
	case when y.dates_active is null then array[t.date_active]
		when t.date_active is not null then y.dates_active || array[t.date_active]
		else y.dates_active
	end as dates_active,
	coalesce(t.date_active, y.date + interval '1 day') as date
from today t
full outer join yesterday y
on t.user_id = y.user_id


-- select date, row_number() over (order by date)
-- from users_cumulated
-- group by 1
