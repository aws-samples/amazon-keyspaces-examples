## Import from S3

This example provides a Scala script for importing data to Amazon Keyspaces from S3 using AWS Glue.

### Prerequisites

* Run `./keyspaces-bulk-cli bootstrap` from the [parent directory](../) to set up infrastructure and deploy all Glue jobs

### Running the Import

```bash
./keyspaces-bulk-cli import \
  --keyspace mykeyspace \
  --table mytable \
  --s3-uri s3://my-bucket/export/mykeyspace/mytable/snapshot/year=2025/month=01/day=15/hour=10/minute=30 \
  --format parquet
```

Override workers for large imports:
```bash
./keyspaces-bulk-cli import \
  --keyspace production \
  --table orders \
  --s3-uri s3://my-bucket/export/production/orders/snapshot/year=2025/month=06/day=01/hour=00/minute=00 \
  --format parquet \
  --workers 10
```

### Script Arguments

| Argument | Description | Default |
| :--- | :--- | :--- |
| --KEYSPACE_NAME | Keyspace containing the target table | mykeyspace |
| --TABLE_NAME | Table to import into | mytable |
| --S3_URI | S3 URI where the source data is located | s3://{bucket}/export |
| --FORMAT | Format: parquet, json, or csv | parquet |
| --DRIVER_CONF | Driver configuration file | keyspaces-application.conf |

### Performance Tuning

When importing data with Glue, consider:

1. **Time duration** — how long the job has to complete
2. **Row rate** — rows per second needed (e.g., 10M rows in 1 hour = ~2,778 rows/sec)
3. **Capacity** — ensure the Keyspaces table has sufficient provisioned or on-demand throughput

**Provisioned mode**: Match Write Capacity Units to your target rate. For rows averaging 5KB, at 2,778 rows/sec you need ~15,500 WCU:

```bash
aws keyspaces update-table --keyspace-name mykeyspace --table-name mytable \
  --capacity-specification "throughputMode=PROVISIONED,readCapacityUnits=10,writeCapacityUnits=15500"
```

**On-demand mode**: For new tables expecting high throughput, create in provisioned mode first at your target rate, then switch to on-demand to set the initial baseline.

**Glue workers**: Each Spark Cassandra connector instance achieves ~1,000 1KB writes/sec per concurrent writer thread. Adjust `spark.cassandra.output.concurrent.writes` (default 3) and number of workers to reach your target throughput.
