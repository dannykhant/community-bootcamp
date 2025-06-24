-- struct for films
create type films as (
	film text,
	votes integer,
	rating real,
	filmid text
);

-- enum for quality class
create type quality_class as enum (
	'star', 'good', 'average', 'bad'
);

-- actors table
create table actors (
	actor_id text,
	actor text,
	current_year integer,
	films films[],
	quality_class quality_class,
	is_active boolean,
	primary key(actor_id, current_year)
)

-- check for the seed query
select min(year) from actor_films

-- cummulative table generation
insert into actors
with aggregated as (select
		actorid as actor_id,
		actor,
		year as current_year,
		array_agg(row(film, votes, rating, filmid)::films) as films,
		avg(rating) as avg_rating
	from actor_films
	group by 1, 2, 3
)
, last_year as (
	select *
	from actors
	where current_year = 1974
)
, this_year as (
	select *
	from aggregated
	where current_year = 1975
)
select
	coalesce(ly.actor_id, ty.actor_id) as actor_id,
	coalesce(ly.actor, ty.actor) as actor,
	coalesce(ty.current_year, ly.current_year + 1) as current_year,
	case when ty.current_year is not null then ty.films
		else array[]::films[]
	end as films,
	case when ty.current_year is not null then
		case when ty.avg_rating > 8 then 'star'
			when ty.avg_rating > 7 then 'good'
			when ty.avg_rating > 6 then 'average'
			else 'bad'
		end::quality_class
		else ly.quality_class
	end as quality_class,
	ty.current_year is not null as is_active
from last_year ly
full outer join this_year ty
on ly.actor_id = ty.actor_id

-- actors_history_scd table
create table actors_history_scd (
	actor_id text,
	actor text,
	quality_class quality_class,
	is_active boolean,
	start_year integer,
	end_year integer,
	current_year integer,
	primary key(actor_id, start_year)
)

-- backfill for actors_history_scd
insert into actors_history_scd
with previous as (
	select
		actor_id,
		actor,
		quality_class,
		is_active,
		lag(quality_class) over 
			(partition by actor_id order by current_year) as previous_quality_class,
		lag(is_active) over
			(partition by actor_id order by current_year) as previous_is_active,
		current_year
	from actors
)
, indicator as (
	select
		*,
		case when quality_class <> previous_quality_class then 1
		 when is_active <> previous_is_active then 1
		 else 0
		end as indicator
	from previous
)
, streak_indicator as (
	select
		*,
		sum(indicator) over (partition by actor_id order by current_year) as streak_indicator
	from indicator
)
select
	actor_id,
	actor,
	quality_class,
	is_active,
	min(current_year) as start_year,
	max(current_year) as end_year,
	1974 as current_year
from streak_indicator
group by 
	actor_id,
	actor,
	streak_indicator,
	quality_class,
	is_active
order by 1, streak_indicator

-- create record type for changed records
create type record_type as (
	quality_class quality_class,
	is_active boolean,
	start_year integer,
	end_year integer
)

-- incremental for actors_history_scd
with last_year as (
	select *
	from actors_history_scd
	where end_year = 1974
	and current_year = 1974
)
, this_year as (
	select *
	from actors
	where current_year = 1975
)
, historical_records as (
	select 
		actor_id,
		actor,
		quality_class,
		is_active,
		start_year,
		end_year
	from actors_history_scd
	where end_year < 1974
	and current_year = 1974
)
, unchanged_records as (
	select
		ly.actor_id,
		ly.actor,
		ly.quality_class,
		ly.is_active,
		ly.start_year,
		ty.current_year as end_year
	from last_year ly
	join this_year ty
	on ly.actor_id = ty.actor_id
	where ly.quality_class = ty.quality_class
	and ly.is_active = ty.is_active
)
, changed_records as (
	select
		ly.actor_id,
		ly.actor,
		unnest(array[(
				ly.quality_class,
				ly.is_active,
				ly.start_year,
				ly.end_year
			)::record_type,
			(
				ty.quality_class,
				ty.is_active,
				ty.current_year,
				ty.current_year
			)::record_type
		]) as records
	from last_year ly
	join this_year ty
	on ly.actor_id = ty.actor_id
	where ly.quality_class <> ty.quality_class
	or ly.is_active <> ty.is_active
)
, unnested_changed_records as (
	select
		actor_id,
		actor,
		(records).quality_class,
		(records).is_active,
		(records).start_year,
		(records).end_year
	from changed_records
)
, new_records as (
	select 
		ty.actor_id,
		ty.actor,
		ty.quality_class,
		ty.is_active,
		ty.current_year as start_year,
		ty.current_year as end_year
	from this_year ty
	left join last_year ly
	on ty.actor_id = ly.actor_id
	where ly.actor_id is null
)
select *, 1975 as current_year from(
	select * from historical_records
	
	union all
	
	select * from unchanged_records
	
	union all
	
	select * from unnested_changed_records
	
	union all
	
	select * from new_records) r
	order by 1, start_year

