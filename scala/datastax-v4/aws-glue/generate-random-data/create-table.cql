CREATE KEYSPACE IF NOT EXISTS aws WITH REPLICATION = {'class': 'SingleRegionStrategy'}

CREATE TABLE IF NOT EXISTS aws.my_table_example (
	"id" text,
	"create_date" timestamp,
	"data" text,
	"count" bigint,
	PRIMARY KEY("id", "create_date"))
WITH CUSTOM_PROPERTIES = {
	'capacity_mode':{
		'throughput_mode':'PAY_PER_REQUEST'
	},
	'point_in_time_recovery':{
		'status':'enabled'
	},
	'encryption_specification':{
		'encryption_type':'AWS_OWNED_KMS_KEY'
	}
} AND CLUSTERING ORDER BY("create_date" ASC)
