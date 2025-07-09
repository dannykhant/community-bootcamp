
with base as (select
	e.user_id,
	e.event_time::timestamp,
	e.url,
	coalesce(d.os_type, 'N/A') as os_type,
	coalesce(d.browser_type, 'N/A') as browser_type,
	case when referrer like '%eczachly%' then 'On Site'
		when referrer like '%zachwilson%' then 'On Site'
		when referrer like '%linkedin%' then 'Linkedin'
		when referrer like '%google%' then 'Google'
		when referrer like '%t.co%' then 'Twitter'
		when referrer like '%facebook%' then 'Facebook'
		when referrer like '%youtube%' then 'Youtube'
		when referrer like '%bing%' then 'Bing'
		when referrer is null then 'Direct'
		else 'Other'
	end as referrer_mapped
from events e
join devices d
on e.device_id = d.device_id)

-- grouping sets
select 
	coalesce(referrer_mapped, '(overall)') as referrer_mapped,
	coalesce(os_type, '(overall)') as os_type,
	coalesce(browser_type, '(overall)') as browser_type,
	count(1) as site_hits,
	count(case when url = '/signup' then 1 end) as signup_visits,
	count(case when url = '/login' then 1 end) as login_visits,
	count(case when url = '/contact' then 1 end) as contact_visits
from base
group by grouping sets (
	(referrer_mapped, os_type, browser_type),
	(referrer_mapped),
	()
)
order by site_hits desc

-- rollup
select 
	coalesce(referrer_mapped, '(overall)') as referrer_mapped,
	coalesce(os_type, '(overall)') as os_type,
	coalesce(browser_type, '(overall)') as browser_type,
	count(1) as site_hits,
	count(case when url = '/signup' then 1 end) as signup_visits,
	count(case when url = '/login' then 1 end) as login_visits,
	count(case when url = '/contact' then 1 end) as contact_visits
from base
group by rollup (referrer_mapped, os_type, browser_type)
order by site_hits desc

-- cube
select 
	coalesce(referrer_mapped, '(overall)') as referrer_mapped,
	coalesce(os_type, '(overall)') as os_type,
	coalesce(browser_type, '(overall)') as browser_type,
	count(1) as site_hits,
	count(case when url = '/signup' then 1 end) as signup_visits,
	count(case when url = '/login' then 1 end) as login_visits,
	count(case when url = '/contact' then 1 end) as contact_visits
from base
group by cube (referrer_mapped, os_type, browser_type)
order by site_hits desc

-- self join
, agg as (select
	b1.user_id,
	b1.url as url_to,
	b2.url as url_from,
	min(b1.event_time - b2.event_time) as duration
from base b1
join base b2
on b1.user_id = b2.user_id
	and date(b1.event_time) = date(b2.event_time)
	and b1.event_time > b2.event_time
group by b1.user_id, b1.url, b2.url)

select
	url_to,
	url_from,
	avg(duration) as avg_duration
from agg
group by url_to, url_from 