-- game_details deduplication
with deduped as (select
		*,
		row_number() over (partition by game_id, team_id, player_id) as rn
	from game_details
)
select 
	game_id as dim_game_id,
	team_id as dim_team_id,
	player_id as dim_player_id,
	player_name as dim_player_name,
	start_position as dim_start_position,
	comment as dim_comment,
	split_part(min, ':', 1)::real + 
		split_part(min, ':', 2)::real/60 as m_min,
	fgm as m_fgm,
	fga as m_fga,
	fg3m as m_fg3m,
	fg3a as m_fg3a,
	ftm as m_ftm,
	fta as m_fta,
	oreb as m_oreb,
	dreb as m_dreb,
	reb as m_reb,
	ast as m_ast,
	stl as m_stl,
	blk as m_blk,
	"TO" as m_turnover,
	pf as m_pf,
	pts as m_pts,
	plus_minus as m_plus_minus
from deduped
where rn = 1

-- type for device_activity_datelist
create type dev_datelist as (
	browser_type text,
	datelist date[]
)

-- ddl for user_devices_cumulated
create table user_devices_cumulated (
	user_id numeric,
	device_id numeric,
	date date,
	device_activity_datelist dev_datelist,
	primary key(user_id, device_id, date)
)

-- cumulative query for user_devices_cumulated
insert into user_devices_cumulated
with yesterday as (
	select *
	from user_devices_cumulated
	where date = date('2023-01-30')
)
, today as (
	select 
		e.user_id,
		e.device_id,
		date(e.event_time) as date_active,
		d.browser_type,
		count(1) as num_site_hits
	from events e
	join devices d
	on e.device_id = d.device_id
	where date(event_time) = date('2023-01-31') 
		and user_id is not null
	group by e.user_id, e.device_id, 
		date(e.event_time), d.browser_type
)
select
	coalesce(t.user_id, y.user_id) as user_id,
	coalesce(t.device_id, y.device_id) as device_id,
	coalesce(t.date_active, y.date + 1) as date,
	case when y.device_activity_datelist is null
			then (t.browser_type, 
				array[t.date_active])::dev_datelist
		when t.date_active is not null
			then ((y.device_activity_datelist).browser_type,
					(y.device_activity_datelist).datelist 
					|| array[t.date_active])::dev_datelist
		else y.device_activity_datelist
	end as device_activity_datelist
from today t
full outer join yesterday y
on t.user_id = y.user_id
	and t.device_id = y.device_id
	and t.browser_type = (y.device_activity_datelist).browser_type

-- type for device_activity_intlist
create type dev_intlist as (
	browser_type text,
	intlist bit(32)
)

-- datelist_int generation
with user_devices as (
	select *
	from user_devices_cumulated
	where date = date('2023-01-31')
)
, series as (
	select *
	from generate_series(date('2023-01-01'), 
							date('2023-01-31'), '1 day') as serie_date
)
, ints as (
	select *,
		case when (device_activity_datelist).datelist @> array[serie_date::date]
				then pow(2, 31 - (date - serie_date::date))
			else 0 
		end as int_value
	from user_devices ud
	cross join series s
)
select
	user_id,
	device_id,
	((device_activity_datelist).browser_type,
		sum(int_value)::bigint::bit(32))::dev_intlist as datelist_int
from ints
group by user_id, device_id, 
			(device_activity_datelist).browser_type

-- ddl for hosts_cumulated
create table hosts_cumulated (
	host text,
	date date,
	host_activity_datelist date[],
	primary key(host, date)
)

-- incremental query for host_activity_datelist
insert into hosts_cumulated
with yesterday as (
	select *
	from hosts_cumulated
	where date = date('2023-01-04')
)
, today as (
	select 
		host,
		date(event_time) date_active,
		count(1) as num_site_hits
	from events
	where date(event_time) = date('2023-01-05')
	group by host, date(event_time)
)
select 
	coalesce(t.host, y.host) as host,
	coalesce(t.date_active, y.date + 1) as date,
	case when y.host_activity_datelist is null
			then array[t.date_active]
		when t.date_active is not null
			then y.host_activity_datelist || array[t.date_active]
		else y.host_activity_datelist
	end as host_activity_datelist
from today t
full outer join yesterday y
on t.host = y.host

-- ddl for host_activity_reduced
create table host_activity_reduced (
	host text,
	month date,
	metric_name text,
	metric_array real[],
	primary key(host, month, metric_name)
)

-- hit_array
insert into host_activity_reduced
with yesterday as (
	select *
	from host_activity_reduced
	where month = date('2023-01-01')
			and metric_name = 'hit_array'
)
, today as (
	select 
		host,
		date(event_time) as date_active,
		count(1) as num_hits,
		count(distinct user_id) num_uniq_visitors
	from events
	where date(event_time) = date('2023-01-03')
	group by host, date(event_time)	
)
select
	coalesce(t.host, y.host) as host,
	coalesce(y.month, date_trunc('month', t.date_active)) as date,
	'hit_array' as metric_name,
	case when y.metric_array is null
			then array_fill(0, 
					array[t.date_active - date_trunc('month', t.date_active)::date])
					|| array[t.num_hits]
		when t.date_active is not null
			then y.metric_array || array[t.num_hits]
		else y.metric_array || array[0]
	end as metric_array
from today t
full outer join yesterday y
on t.host = y.host
on conflict(host, month, metric_name)
do
	update set metric_array = excluded.metric_array;

-- unique_visitors_array
insert into host_activity_reduced
with yesterday as (
	select *
	from host_activity_reduced
	where month = date('2023-01-01')
			and metric_name = 'unique_visitor_array'
)
, today as (
	select 
		host,
		date(event_time) as date_active,
		count(1) as num_hits,
		count(distinct user_id) num_uniq_visitors
	from events
	where date(event_time) = date('2023-01-03')
	group by host, date(event_time)	
)
select
	coalesce(t.host, y.host) as host,
	coalesce(y.month, date_trunc('month', t.date_active)) as date,
	'unique_visitor_array' as metric_name,
	case when y.metric_array is null
			then array_fill(0, 
					array[t.date_active - date_trunc('month', t.date_active)::date]) 
					|| array[t.num_uniq_visitors]
		when t.date_active is not null
			then y.metric_array || array[t.num_uniq_visitors]
		else y.metric_array || array[0]
	end as metric_array
from today t
full outer join yesterday y
on t.host = y.host
on conflict(host, month, metric_name)
do
	update set metric_array = excluded.metric_array;




