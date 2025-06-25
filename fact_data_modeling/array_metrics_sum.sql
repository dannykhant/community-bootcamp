-- array length check
select 
	cardinality(metric_array), count(1)
from array_metrics
group by 1

-- daily hits
with agg as (
	select
		metric_name,
		month_start,
		array[
			sum(metric_array[1]),
			sum(metric_array[2]),
			sum(metric_array[3]),
			sum(metric_array[4]),
			sum(metric_array[5])
		] as sum_arr
	from array_metrics
	group by 1, 2
)
select 
	metric_name,
	month_start + (idx - 1 || ' day')::interval as date,
	e as value
from agg
cross join unnest(agg.sum_arr)
	with ordinality as a(e, idx)
