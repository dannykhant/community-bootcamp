-- // create a scd table
-- create table players_scd (
-- 	player_name text,
-- 	scoring_class scoring_class,
-- 	is_active boolean,
-- 	start_season integer,
-- 	end_season integer,
-- 	current_season integer,
-- 	primary key(player_name, start_season)
-- )

-- // scd initial generation
insert into players_scd
with with_previous as (select
	player_name,
	current_season,
	scoring_class,
	lag(scoring_class) over (partition by player_name 
		order by current_season) as previous_scoring_class,
	is_active,
	lag(is_active) over (partition by player_name
		order by current_season) as previous_is_active
from players
where current_season <= 2004)

, with_indicator as (select *,
	case when scoring_class <> previous_scoring_class then 1
		when is_active <> previous_is_active then 1
 		else 0
	end as change_indicator
from with_previous)

, with_streaks as (select *,
	sum(change_indicator) over (partition by player_name
		order by current_season) as streak_identifier
from with_indicator)

select
	player_name,
	scoring_class,
	is_active,
	min(current_season) as start_season,
	max(current_season) as end_season,
	2004 as current_season
from with_streaks
group by player_name, streak_identifier, scoring_class, is_active
order by 1, streak_identifier



