## Export to S3

This example provides a Scala script for exporting Amazon Keyspaces table data to S3 using AWS Glue.

### Prerequisites

* Run `./keyspaces-glue bootstrap` from the [parent directory](../) to set up infrastructure and deploy all Glue jobs

### S3 Output Structure

Exports are written to a Hive-partitioned path under your S3 bucket:

```
s3://{bucket}/export/{keyspace}/{table}/snapshot/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/
```

### Running the Export

```bash
./keyspaces-glue export \
  --keyspace mykeyspace \
  --table mytable \
  --s3-uri s3://my-bucket \
  --format parquet
```

With a where clause to export a subset:
```bash
./keyspaces-glue export \
  --keyspace mykeyspace \
  --table mytable \
  --s3-uri s3://my-bucket \
  --where-clause "created_date >= '2025-01-01'"
```

Override workers for large tables:
```bash
./keyspaces-glue export \
  --keyspace production \
  --table orders \
  --s3-uri s3://my-bucket \
  --workers 10
```

### Script Arguments

| Argument | Description | Default |
| :--- | :--- | :--- |
| --KEYSPACE_NAME | Keyspace containing the table to export | mykeyspace |
| --TABLE_NAME | Table to export | mytable |
| --S3_URI | S3 URI root for export output | s3://{bucket}/export |
| --FORMAT | Export format: parquet, json, or csv | parquet |
| --WHERE_CLAUSE | Optional filter condition | (none) |
| --DRIVER_CONF | Driver configuration file | keyspaces-application.conf |

### Scheduled Trigger (Cron)

You can trigger this export regularly using a Glue scheduled trigger:

```bash
aws glue create-trigger \
  --name KeyspacesExportWeeklyTrigger \
  --type SCHEDULED \
  --schedule "cron(0 12 ? * MON *)" \
  --start-on-creation \
  --actions '[{
     "JobName": "AmazonKeyspacesExportToS3-aksglue-aksglue-export",
     "Arguments": {
       "--KEYSPACE_NAME": "production",
       "--TABLE_NAME": "transactions"
     }
  }]'
```
