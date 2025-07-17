-- What is the average number of web events of a session from a user?
-- ip				host					avg_events
-- "118.175.253.49"	"www.techcreator.io"		15
-- "103.16.71.197"	"datagym.techcreator.io"	5
-- "99.55.161.102"	"zachwilson.techcreator.io"	5
select 
	ip, host, 
	avg(num_hits)::int avg_events 
from processed_user_session_events_agg
where host like '%techcreator.io'
group by ip, host
order by avg(num_hits) desc

-- Compare results between different hosts
-- host				num_sessions total_events
-- "lulu.techcreator.io"		3	6
-- "zachwilson.techcreator.io"	2	8
select 
	host, 
	count(distinct ip) as num_sessions, 
	sum(num_hits) as total_events
from processed_user_session_events_agg
where host in ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
group by host
order by count(distinct ip) desc