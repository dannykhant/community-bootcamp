-- // check source
-- select * from player_seasons

-- // create struct
-- create type season_stats as (
-- 	season integer,
-- 	gp integer,
-- 	pts real,
-- 	reb real,
-- 	ast real
-- )

-- // create enum
-- create type scoring_class as enum('star', 'good', 'average', 'bad')

-- // create destination
-- create table players (
-- 	player_name text,
-- 	height text,
-- 	weight integer,
-- 	college text,
-- 	country text,
-- 	draft_year text,
-- 	draft_round text,
-- 	draft_number text,
-- 	season_stats season_stats[],
-- 	scoring_class scoring_class,
-- 	years_since_last_season integer,
-- 	current_season integer,
-- 	primary key(player_name, current_season)
-- )

-- // Check to create seed query
-- select min(season) from player_seasons

-- // Main query
insert into players
with yesterday as (
	select *
	from players
	where current_season = 2000
)
, today as (
	select *
	from player_seasons
	where season = 2001
)
select 
	coalesce(t.player_name, y.player_name) player_name,
	coalesce(t.height, y.height) height,
	coalesce(t.weight, y.weight) weight,
	coalesce(t.college, y.college) college,
	coalesce(t.country, y.country) country,
	coalesce(t.draft_year, y.draft_year) draft_year,
	coalesce(t.draft_round, y.draft_round) draft_round,
	coalesce(t.draft_number, y.draft_number) draft_number,
	case when y.season_stats is null then array[Row(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
		)::season_stats]
		when t.season is not null then y.season_stats || array[Row(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
		)::season_stats]
		else y.season_stats
	end as season_stats,
	case when t.season is not null then 
			case when t.pts > 20 then 'star'
				when t.pts > 15 then 'good'
				when t.pts > 10 then 'average'
				else 'bad'
			end::scoring_class
		else y.scoring_class
	end as scoring_class,
	case when t.season is not null then 0
		else y.years_since_last_season + 1
	end as years_since_last_season,
	coalesce(t.season, y.current_season + 1) as current_season
from today t
full outer join yesterday y
on t.player_name = y.player_name

-- // Extract the struct
-- select 
-- 	player_name,
-- 	(unnest(season_stats)).*
-- from players 
-- where current_season = 2001 and scoring_class = 'star'

-- // Analysis
-- select
-- 	player_name,
-- 	(season_stats[cardinality(season_stats)]).pts /
-- 	case when (season_stats[1]).pts = 0 then 1 else (season_stats[1]).pts end
-- from players 
-- where current_season = 2001
-- order by 4 desc
