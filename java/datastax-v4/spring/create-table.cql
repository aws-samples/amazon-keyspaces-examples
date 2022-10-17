CREATE TABLE "aws"."user"(
	"username" text,
	"fname" text,
	"lname" text,
	PRIMARY KEY("username"))
WITH default_time_to_live = 1209600 AND CUSTOM_PROPERTIES = {
	'capacity_mode':{
		'throughput_mode':'PAY_PER_REQUEST'
	},
	'point_in_time_recovery':{
		'status':'enabled'
	},
	'encryption_specification':{
		'encryption_type':'AWS_OWNED_KMS_KEY'
	}
} AND TAGS = {'amazon-keyspaces-example' : 'spring-boot'}