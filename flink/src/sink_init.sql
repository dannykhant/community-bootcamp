create table processed_events (
	ip varchar,
	event_timestamp timestamp(3),
	referrer varchar,
	host varchar,
	url varchar,
	geodata varchar
)


create table processed_events_aggregated (
	event_hour timestamp(3),
	host varchar,
	num_hits bigint
)


create table processed_events_aggregated_referrer (
	event_hour timestamp(3),
	host varchar,
	referrer varchar,
	num_hits bigint
)