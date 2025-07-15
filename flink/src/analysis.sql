select 
	geodata::json->>'country' as country,
	count(1)
from processed_events
group by geodata::json->>'country'
order by 2 desc