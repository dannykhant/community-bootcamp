select 
	v.properties->>'player_name',
	max(cast(e.properties->>'pts' as integer))
from vertices v
join edges e
on e.subject_identifier = v.identifier
and e.subject_type = v.type
where e.object_type = 'game'::vertex_type
group by 1
order by 2 desc


select
	v.properties->>'player_name',
	cast(v.properties->>'num_of_games' as real) /
	case when cast(v.properties->>'total_points' as real) = 0 then 1
		else cast(v.properties->>'total_points' as real) end,
	e.properties->>'subject_pts',
	e.properties->>'num_games'
from vertices v
join edges e
on v.identifier = e.subject_identifier
and v.type = e.subject_type
where e.object_type = 'player'::vertex_type