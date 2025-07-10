-- ddl for state change tracking
create table player_state (
	player_name text,
	first_active_season int,
	last_active_season int,
	current_state text,
	active_seasons int[],
	current_season int,
	primary key(player_name, current_season)
) 

-- player state change tracking query
insert into player_state
with yesterday as (
	select
		*
	from player_state
	where current_season = 1997
)
, today as (
	select
		*
	from player_seasons
	where season = 1998
)
select
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(y.first_active_season, t.season) as first_active_season,
	coalesce(t.season, y.last_active_season) as last_active_season,
	case when y.player_name is null 
			then 'New'
		when y.last_active_season = t.season - 1
			then 'Continued Playing'
		when y.last_active_season < t.season - 1
			then 'Returned from Retirement'
		when t.season is null 
			and y.last_active_season = y.current_season
			then 'Retired'
		else 'Stayed Retired'
	end as current_state,
	case when y.active_seasons is null
			then array[t.season]
		when t.season is not null
			then y.active_seasons || t.season
		else y.active_seasons
	end as active_seasons,
	coalesce(t.season, y.current_season + 1) as current_season
from today t
full outer join yesterday y
on t.player_name = y.player_name


with winner as (select
	gd.*,
	g.game_date_est,
	g.season,
	case when g.home_team_wins = 1 then g.home_team_id
		else g.visitor_team_id
	end as winner_team
from game_details gd
join games g
on gd.game_id = g.game_id
where gd.pts is not null)

-- grouping sets query
, agg as (
	select
		coalesce(cast(season as text), '(all seasons)') as season,
		coalesce(team_city, '(overall)') as team_city,
		coalesce(player_name, '(overall)') as player_name,
		sum(pts) as total_points,
		count(distinct case when team_id = winner_team then game_id end) as games_won
	from winner
	group by grouping sets (
		(player_name, team_city),
		(player_name, season),
		(team_city)
	)
	order by sum(pts) desc
)

-- who scored the most points for Boston
select 
	team_city, player_name, total_points
from agg
where player_name <> '(overall)'
 and team_city = 'Boston'

-- who scored the most points in 2021
select
	season, player_name, total_points
from agg
where season = '2021'

-- which team won most games in all seasons
select
	season,
	team_city,
	games_won
from agg
where season = '(all seasons)' and player_name = '(overall)'
order by season, games_won desc

-- most games a team has won in a 90-game stretch
, dedup as (
	select
		team_city,
		case when team_id = winner_team 
			then game_id end as game_won,
		game_date_est
	from  winner
	group by team_city,
		case when team_id = winner_team 
			then game_id end,
		game_date_est
)
, windowed as (
	select
		team_city,
		game_date_est,
		count(game_won) over (
				partition by team_city 
				order by game_date_est
				rows between 89 preceding and current row) as games_won,
		count(1) over (
				partition by team_city 
				order by game_date_est
				rows between 89 preceding and current row) as total_games
	from dedup
	order by team_city, game_date_est
)
select
	team_city,
	max(games_won) as most_games_won
from windowed
where total_games = 90
group by team_city
order by max(games_won) desc

-- num of games in a row LeBron James score over 10 points
, player_windowed as (select 
	player_name,
	game_date_est,
	pts,
	case when pts <= 10 then '10 or below' else 'above 10' end pts_flag,
	sum(case when pts <= 10 then 1 else 0 end) 
		over (partition by player_name order by game_date_est
				rows between unbounded preceding and current row) as group_id
from winner
where player_name = 'LeBron James')

select
	player_name,
	min(game_date_est) as start_date,
	max(game_date_est) as end_date,
	count(1) as num_games
from player_windowed
group by player_name, group_id
order by min(game_date_est)





