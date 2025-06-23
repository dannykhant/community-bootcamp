-- plays_in
insert into edges
with deduped_game as (
	select *, row_number() over (partition by player_id, game_id) rn
	from game_details
)
select 
	player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	game_id as object_identifier,
	'game'::vertex_type as object_type,
	'plays_in'::edge_type as edge_type,
	json_build_object(
		'start_position', start_position,
		'pts', pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
	) as properties
from deduped_game
where rn = 1

-- shares_team, plays_against
insert into edges
with deduped_game as (
	select *, row_number() over (partition by player_id, game_id) rn
	from game_details
)
, filtered_game as (
	select * from deduped_game
	where rn = 1
)
, aggregated as (select 
	g1.player_id as subject_player_id,
	g2.player_id as object_player_id,
	case when g1.team_abbreviation = g2.team_abbreviation then 'shares_team'::edge_type
		else 'plays_against'::edge_type end as edge_type,
	max(g1.player_name) as subject_player_name,
	max(g2.player_name) as object_player_name,
	count(1) num_games,
	sum(g1.pts) as subject_pts,
	sum(g2.pts) as object_pts
from filtered_game g1
join filtered_game g2
on g1.game_id = g2.game_id
and g1.player_id <> g2.player_id
where g1.player_id < g2.player_id
group by 1, 2, 3)

select
	 subject_player_id as subject_identifier,
	 'player'::vertex_type as subject_type,
	 object_player_id as object_identifier,
	 'player'::vertex_type as object_type,
	 edge_type,
	 json_build_object(
		'num_games', num_games,
		'subject_pts', subject_pts,
		'object_pts', object_pts
	 ) as properties
from aggregated



