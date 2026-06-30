## Random Compare — Incremental Export with Token Sampling

This example extends the [incremental export](../incremental-export-to-s3) with random token-range sampling for validation. It compares two S3 snapshots to produce an incremental diff of inserts, updates, and deletes. The export script includes helper methods to select random token ranges from the Cassandra ring for sampling-based verification.

### Prerequisites

* Run `./keyspaces-bulk-cli bootstrap` from the [parent directory](../) to set up infrastructure and deploy all Glue jobs
* Two existing snapshots in S3 (created by the export job)

### S3 Output Structure

```
s3://{bucket}/export/{keyspace}/{table}/snapshot/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/
s3://{bucket}/export/{keyspace}/{table}/incremental/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/
```

### Workflow

```bash
# 1. Bootstrap
./keyspaces-bulk-cli bootstrap --stack aksglue

# 2. First export (baseline)
./keyspaces-bulk-cli export --keyspace mykeyspace --table mytable --s3-uri s3://my-bucket

# 3. Second export (after changes)
./keyspaces-bulk-cli export --keyspace mykeyspace --table mytable --s3-uri s3://my-bucket

# 4. Compute incremental diff
./keyspaces-bulk-cli incremental-export \
  --keyspace mykeyspace --table mytable \
  --past-uri s3://my-bucket/export/mykeyspace/mytable/snapshot/year=2025/month=01/day=01/hour=00/minute=00 \
  --current-uri s3://my-bucket/export/mykeyspace/mytable/snapshot/year=2025/month=01/day=02/hour=00/minute=00 \
  --s3-uri s3://my-bucket \
  --distinct-keys "partition_id,row_id"

# 5. Import the diff into a target table
./keyspaces-bulk-cli incremental-import \
  --keyspace target_keyspace --table mytable \
  --s3-uri s3://my-bucket/export/mykeyspace/mytable/incremental/year=2025/month=01/day=02/hour=00/minute=30 \
  --distinct-keys "partition_id,row_id"
```

### Diff Format

| Code | Meaning | Description |
| :--- | :--- | :--- |
| I | INSERT | New row |
| D | DELETE | Removed row |
| C | CHANGE | Updated row (both old and new values) |
| N | NO CHANGE | Filtered out before writing |

```json
{"diff":"I","id":"-67214788834858333A","create_date":"2023-08-01T18:23:59.476Z","right_count":1,"right_data":"-67214788834858333A"}
{"diff":"D","id":"-7134365464711695755","create_date":"2023-02-25T19:08:26.000Z","left_count":-823388298404028328,"left_data":"-7444764033894103459"}
{"diff":"C","id":"-672147888348583332","create_date":"2023-02-27T20:04:30.000Z","left_count":-2365493309516205255,"right_count":1,"left_data":"-1643515377232023460","right_data":"-1643515377232023460"}
```

### Chaining with Glue Workflows

You can chain export, incremental export, and incremental import through a Glue workflow triggered by Amazon EventBridge events (S3 PutObject).
