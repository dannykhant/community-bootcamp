-- game
insert into vertices
select 
	game_id as identifier,
	'game'::vertex_type as type,
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', case when home_team_wins = 1 then home_team_id
							else visitor_team_id end
	) as properties
from games

-- player
insert into vertices
with player_agg as (select 
	player_id,
	max(player_name) as player_name,
	count(1) as num_of_games,
	sum(pts) as total_points,
	array_agg(distinct team_id) as teams
from game_details
group by player_id)

select
	player_id as identifier,
	'player'::vertex_type as type,
	json_build_object(
		'player_name', player_name,
		'num_of_games', num_of_games,
		'total_points', total_points,
		'teams', teams
	) as properties
from player_agg

-- team
insert into vertices
with deduped_teams as (
	select
		*, row_number() over (partition by team_id) as rn
	from teams
)
select
	team_id as identifier,
	'team'::vertex_type as type,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_founded', yearfounded
	) as properties
from deduped_teams
where rn = 1
order by team_id

-- check result
select type, count(1)
from vertices
group by 1
