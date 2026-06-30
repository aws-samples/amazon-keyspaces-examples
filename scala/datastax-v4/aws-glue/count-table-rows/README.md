## Count Table Rows

This example provides a Scala script for counting the number of rows in an Amazon Keyspaces table using AWS Glue.

### Prerequisites

* Run `./keyspaces-bulk-cli bootstrap` from the [parent directory](../) to set up infrastructure and deploy all Glue jobs

### Count All Rows

```bash
./keyspaces-bulk-cli count --keyspace mykeyspace --table mytable
```

Result is logged to CloudWatch:
```
Total number of rows: 5191983
```

### Count Distinct Keys

Provide a comma-separated list of columns to count distinct combinations:

```bash
./keyspaces-bulk-cli count --keyspace mykeyspace --table mytable --distinct-keys "user_id"
```

Result:
```
Total number of distinct rows: 41983
```

### Count with a Filter

```bash
./keyspaces-bulk-cli count --keyspace mykeyspace --table mytable \
  --where-clause "created_date >= '2025-01-01'"
```

### Script Arguments

| Argument | Description | Default |
| :--- | :--- | :--- |
| --KEYSPACE_NAME | Keyspace containing the table | mykeyspace |
| --TABLE_NAME | Table to count | mytable |
| --DISTINCT_KEYS | Comma-separated columns for distinct count | (none — counts all rows) |
| --WHERE_CLAUSE | Optional filter condition | (none) |
| --DRIVER_CONF | Driver configuration file | keyspaces-application.conf |

### Viewing Results

Count results are written to CloudWatch logs. Use the CLI to retrieve them:

```bash
./keyspaces-bulk-cli logs count --log-type error
```

For a specific run:
```bash
./keyspaces-bulk-cli logs count --run-id jr_abc123 --log-type error
```
