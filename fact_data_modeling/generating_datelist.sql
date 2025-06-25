with users as (
	select *
	from users_cumulated
	where date = date('2023-01-31')
)
, series as (
	select *
	from generate_series(date('2023-01-01'), 
		date('2023-01-31'), interval '1 day') as series_date
)
, placeholder_ints as (
	select *,
		case when users.dates_active @> array[date(series_date)] then
			pow(2, 31 - (date - date(series_date)))
			else 0
		end as placeholder_int_value
	from users
	cross join series
)
, bits as (
	select
		user_id,
		sum(placeholder_int_value)::bigint::bit(32) date_bits
	from placeholder_ints
	group by user_id
)
select
	user_id,
	bit_count(date_bits) > 0 as dim_is_monthly_active,
	bit_count('11111110000000000000000000000000'::bit(32) & date_bits) > 0 as dim_is_weekly_active,
	bit_count('10000000000000000000000000000000'::bit(32) & date_bits) > 0 as dim_is_daily_active
from bits


