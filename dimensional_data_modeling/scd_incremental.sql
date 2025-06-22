-- create type scd_type as (scoring_class scoring_class,
-- 	is_active boolean, 
-- 	start_season integer, 
-- 	end_season integer)

with last_season_scd as (
	select * from players_scd
	where current_season = 2004
		and end_season = 2004
)
, historical_scd as (
	select 
		player_name,
		scoring_class,
		is_active,
		start_season,
		end_season
	from players_scd
	where current_season = 2004
		and end_season < 2004
)
, this_season_data as (
	select * from players
	where current_season = 2005
)
, unchanged_records as (
	select 
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ls.start_season,
		ts.current_season as end_season
	from last_season_scd ls
	join this_season_data ts
	on ls.player_name = ts.player_name
	where ls.scoring_class = ts.scoring_class
		and ls.is_active = ts.is_active
)
, changed_records as (
	select
		ts.player_name,
		unnest(array[
			(
				ls.scoring_class,
				ls.is_active,
				ls.start_season,
				ls.end_season
			)::scd_type,
			(
				ts.scoring_class,
				ts.is_active,
				ts.current_season,
				ts.current_season
			)::scd_type
		]) records
	from last_season_scd ls
	join this_season_data ts
	on ls.player_name = ts.player_name
	where ls.scoring_class <> ts.scoring_class
		or ls.is_active <> ts.is_active
)
, unnested_changed_records as (
	select
		player_name,
		(records).scoring_class,
		(records).is_active,
		(records).start_season,
		(records).end_season
	from changed_records
)
, new_records as (
	select
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ts.current_season,
		ts.current_season
	from this_season_data ts
	left join last_season_scd ls
	on ts.player_name = ls.player_name
	where ts.player_name is null
)

select *, 2005 as current_season
from (select *
	from historical_scd
	
	union all
	
	select *
	from unchanged_records
	
	union all
	
	select *
	from unnested_changed_records
	
	union all
	
	select *
	from new_records) d
	order by 1, 4