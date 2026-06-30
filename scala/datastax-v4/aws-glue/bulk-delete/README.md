## Bulk Delete

This example provides a Scala script for bulk deleting data from Amazon Keyspaces using AWS Glue. Supports full table truncation or filtered deletes with optional S3 backup of deleted data.

### Prerequisites

* Run `./keyspaces-bulk-cli bootstrap` from the [parent directory](../) to set up infrastructure and deploy all Glue jobs

### S3 Backup Structure

If an S3 URI is provided, deleted data is backed up before deletion:

```
s3://{bucket}/export/{keyspace}/{table}/bulk-delete/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/
```

### Running Bulk Delete

Delete all rows (truncate) with backup:
```bash
./keyspaces-bulk-cli bulk-delete \
  --keyspace mykeyspace \
  --table mytable \
  --s3-uri s3://my-bucket
```

Delete with a filter condition:
```bash
./keyspaces-bulk-cli bulk-delete \
  --keyspace mykeyspace \
  --table mytable \
  --where-clause "event_date < '2024-01-01'" \
  --s3-uri s3://my-bucket
```

Range delete by partial key:
```bash
./keyspaces-bulk-cli bulk-delete \
  --keyspace mykeyspace \
  --table mytable \
  --distinct-keys "partition_key" \
  --where-clause "status == 'expired'"
```

### Script Arguments

| Argument | Description | Default |
| :--- | :--- | :--- |
| --KEYSPACE_NAME | Keyspace containing the table | mykeyspace |
| --TABLE_NAME | Table to delete from | mytable |
| --DISTINCT_KEYS | Comma-separated key columns for delete. If blank, uses full primary key. Partial keys trigger range deletes (up to 1000 rows per range). | full primary key |
| --WHERE_CLAUSE | Filter condition to limit which rows are deleted | (none — deletes all rows) |
| --S3_URI | S3 URI for backup of deleted data | (none — no backup) |
| --FORMAT | Backup format: parquet, json, csv | parquet |
| --DRIVER_CONF | Driver configuration file | keyspaces-application.conf |

### Scheduled Trigger (Cron)

For recurring deletes (custom TTL, cold data cleanup):

```bash
aws glue create-trigger \
  --name KeyspacesBulkDeleteWeeklyTrigger \
  --type SCHEDULED \
  --schedule "cron(0 12 ? * MON *)" \
  --start-on-creation \
  --actions '[{
     "JobName": "AmazonKeyspacesBulkDelete-aksglue-bulk-delete",
     "Arguments": {
       "--KEYSPACE_NAME": "production",
       "--TABLE_NAME": "events",
       "--WHERE_CLAUSE": "event_date < '\''2024-05-17'\''"
     }
  }]'
```
