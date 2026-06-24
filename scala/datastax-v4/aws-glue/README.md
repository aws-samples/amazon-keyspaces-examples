## Amazon Keyspaces Glue CLI

A CLI for launching, monitoring, and managing AWS Glue jobs that operate on Amazon Keyspaces tables. Instead of navigating the AWS Console or writing `aws glue start-job-run` commands with long JSON argument strings, use simple named commands like `export`, `count`, or `compress-partition`.

### Prerequisites

- Python 3.8+
- AWS credentials configured (via `~/.aws/credentials`, environment variables, or IAM role)
- For `bootstrap`: `git`, `mvn` (Maven), and `curl` must be installed

### Quick Start

```bash
git clone https://github.com/aws-samples/amazon-keyspaces-examples.git
cd amazon-keyspaces-examples/scala/datastax-v4/aws-glue

# Bootstrap everything: infrastructure, JARs, configs, and Glue jobs
./keyspaces-glue bootstrap --stack aksglue

# Run your first export
./keyspaces-glue export --keyspace mykeyspace --table mytable --s3-uri s3://my-bucket
```

The CLI automatically creates a virtual environment and installs dependencies on first run.

### Existing Setup (jobs already deployed)

```bash
# Auto-discover your stack from existing Glue jobs and save it
./keyspaces-glue config --discover

# Export a table to S3
./keyspaces-glue export --keyspace mykeyspace --table mytable --s3-uri s3://my-bucket

# Check run status (defaults to latest run)
./keyspaces-glue status export

# View logs
./keyspaces-glue logs export --log-type error
```

## Bootstrap

The `bootstrap` command sets up the entire Keyspaces Glue infrastructure from scratch.

### What It Does

1. **Creates base infrastructure** via CloudFormation (S3 bucket + IAM service role)
2. **Downloads connector JARs** from Maven Central:
   - `spark-cassandra-connector-assembly_2.12-3.1.0.jar`
   - `spark-extension_2.12-2.8.0-3.4.jar`
   - `aws-sigv4-auth-cassandra-java-driver-plugin-4.0.9-shaded.jar`
3. **Builds the retry policy helper** by cloning and compiling `amazon-keyspaces-java-driver-helpers`
4. **Uploads all JARs** to `s3://{bucket}/jars/`
5. **Uploads config files** (`cassandra-application.conf`, `keyspaces-application.conf`) to `s3://{bucket}/conf/`
6. **Deploys all Glue jobs** (export, import, count, bulk-delete, modify-ttl, incremental-export, incremental-import, top-partitions, compress-partition)
7. **Saves the stack name** to `.keyspaces-glue.json` so subsequent commands auto-resolve

### Usage

```bash
# Minimal — uses all defaults (stack=aksglue, bucket auto-named, etc.)
./keyspaces-glue bootstrap

# Custom stack name
./keyspaces-glue bootstrap --stack myproject

# Full customization
./keyspaces-glue bootstrap \
  --stack myproject \
  --bucket my-custom-bucket-name \
  --role-name my-glue-role \
  --keyspace production \
  --table events \
  --format parquet \
  --region us-west-2

# Infrastructure only (skip Glue job creation)
./keyspaces-glue bootstrap --stack myproject --skip-jobs
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--stack` | `aksglue` | CloudFormation stack name prefix |
| `--bucket` | `amazon-keyspaces-glue-{stack}-{account_id}` | S3 bucket for artifacts |
| `--role-name` | `amazon-keyspaces-glue-servcie-role-{stack}` | IAM service role name |
| `--keyspace` | `mykeyspace` | Default keyspace for deployed jobs |
| `--table` | `mytable` | Default table for deployed jobs |
| `--s3-uri` | `s3://{bucket}/export` | Default S3 path for export/import |
| `--format` | `parquet` | Default data format |
| `--skip-jobs` | `false` | Only create infrastructure, skip Glue job deployment |
| `--region` | (from AWS config) | AWS region |
| `--profile` | (from AWS config) | AWS named profile |

### Idempotency

Running `bootstrap` multiple times is safe. If a CloudFormation stack already exists, it is skipped with a warning rather than failing.

## Stack Configuration

The CLI needs to know which CloudFormation stack name was used when deploying the Glue jobs. This stack name is used to derive the full Glue job name automatically.

### Resolution Order

1. `--stack <name>` flag on the command
2. `KEYSPACES_GLUE_STACK` environment variable
3. `.keyspaces-glue.json` config file (checked in current directory, then home directory)
4. **Auto-discover** from existing Glue jobs in the account

### Setting the Stack

```bash
# Option 1: Auto-discover from your account's Glue jobs
./keyspaces-glue config --discover

# Option 2: Set explicitly
./keyspaces-glue config --stack mystack

# Option 3: Environment variable
export KEYSPACES_GLUE_STACK=aksglue

# Option 4: Pass on every command
./keyspaces-glue export --stack mystack --keyspace mykeyspace --table mytable --s3-uri s3://bucket
```

## Commands

### Job Commands

| Command | Description |
|---------|-------------|
| `export` | Export a table to S3 |
| `import` | Import from S3 into a table |
| `count` | Count rows in a table |
| `bulk-delete` | Bulk delete rows |
| `modify-ttl` | Modify TTL values |
| `incremental-export` | Diff two S3 snapshots |
| `incremental-import` | Import incremental diff into a table |
| `generate` | Generate random test data |
| `compress-partition` | Compress partitions into a target table |
| `top-partitions` | Find largest partitions |

### Lifecycle Commands

All lifecycle commands take a module name as the first argument (e.g. `count`, `export`, `compress-partition`). They default to the latest run.

| Command | Description |
|---------|-------------|
| `jobs` | List all Keyspaces Glue jobs in the account |
| `runs <module>` | List recent runs for a job |
| `status <module>` | Get detailed status of the latest run |
| `logs <module>` | Fetch CloudWatch logs for the latest run |
| `cancel <module>` | Cancel the latest running job |

Optional overrides for all lifecycle commands:

| Option | Description |
|--------|-------------|
| `--run-id` | Specify a run ID instead of using the latest |
| `--job-name` | Override with a full Glue job name instead of resolving from module |

### Configuration

| Command | Description |
|---------|-------------|
| `config --stack <name>` | Save stack name to `.keyspaces-glue.json` |
| `config --discover` | Auto-detect stack from existing Glue jobs |
| `config --show` | Display current config and resolution order |

## Usage Examples

### Export and Import Workflow

```bash
# Export table to S3 as parquet
./keyspaces-glue export \
  --keyspace production \
  --table users \
  --s3-uri s3://my-data-bucket \
  --format parquet

# Import data back into a different table
./keyspaces-glue import \
  --keyspace production \
  --table users_restored \
  --s3-uri s3://my-data-bucket/export/production/users/snapshot/year=2025/month=01/day=15/hour=10/minute=30 \
  --format parquet
```

### Count Rows

```bash
# Total row count
./keyspaces-glue count \
  --keyspace production \
  --table orders

# Distinct count on specific columns
./keyspaces-glue count \
  --keyspace production \
  --table orders \
  --distinct-keys "customer_id,product_id"
```

### Bulk Delete with Backup

```bash
./keyspaces-glue bulk-delete \
  --keyspace production \
  --table events \
  --where-clause "status == 'expired'" \
  --s3-uri s3://my-backup-bucket \
  --format parquet
```

### Modify TTL

```bash
# Add 30 days (default) to TTL
./keyspaces-glue modify-ttl \
  --keyspace production \
  --table sessions \
  --ttl-field ttl

# Subtract 7 days from TTL
./keyspaces-glue modify-ttl \
  --keyspace production \
  --table sessions \
  --ttl-field ttl \
  --ttl-time-to-add -604800
```

### Incremental Export (Diff Two Snapshots)

```bash
./keyspaces-glue incremental-export \
  --keyspace production \
  --table orders \
  --past-uri s3://my-bucket/export/production/orders/snapshot/year=2025/month=01/day=01 \
  --current-uri s3://my-bucket/export/production/orders/snapshot/year=2025/month=01/day=15 \
  --s3-uri s3://my-bucket/export \
  --distinct-keys "order_id,customer_id"
```

### Compress Partitions

```bash
./keyspaces-glue compress-partition \
  --keyspace analytics \
  --source-table raw_events \
  --target-table compressed_events \
  --compression ZSTD

# Compress only data older than a date
./keyspaces-glue compress-partition \
  --keyspace analytics \
  --source-table raw_events \
  --target-table compressed_events \
  --compression ZSTD \
  --where-clause "timestamp < '2026-01-01'"
```

### Find Top Partitions

```bash
./keyspaces-glue top-partitions \
  --keyspace production \
  --table orders \
  --group-by "customer_id" \
  --max-results 20 \
  --min-size 1000
```

### Monitor a Job Run

```bash
# List recent runs
./keyspaces-glue runs export

# Get detailed status of latest run
./keyspaces-glue status export

# View output logs (latest run)
./keyspaces-glue logs export

# View error logs (latest run)
./keyspaces-glue logs export --log-type error

# Specific run ID
./keyspaces-glue logs count --run-id jr_abc123
./keyspaces-glue status count --run-id jr_abc123

# Override with full job name
./keyspaces-glue logs count --job-name AmazonKeyspacesCount-aksglue-count --run-id jr_abc123

# Cancel the latest running job
./keyspaces-glue cancel export
```

### Override Workers for Large Jobs

```bash
./keyspaces-glue export \
  --keyspace production \
  --table large_table \
  --s3-uri s3://my-bucket \
  --workers 10
```

## Global Options

All commands support:

| Option | Description |
|--------|-------------|
| `--region` | AWS region (overrides default from AWS config) |
| `--profile` | AWS named profile (from `~/.aws/config`) |
| `--workers` | Override number of Glue workers |

## Region Configuration

Update `keyspaces-application.conf` for your AWS region:
  * __basic.contact-points__ - Amazon Keyspaces service endpoint
  * __basic.load-balancing-policy.local-datacenter__ - Load balancing policy
  * __advanced.auth-provider.aws-region__ - Sigv4 auth provider

## Update the Partitioner

To use Apache Spark or AWS Glue you may need to update the partitioner. Execute this from the Amazon Keyspaces console [CQL editor](https://console.aws.amazon.com/keyspaces/home#cql-editor):

```sql
SELECT partitioner FROM system.local;

UPDATE system.local set partitioner='org.apache.cassandra.dht.Murmur3Partitioner' where key='local';
```

For more info see [Working with partitioners](https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html)

## Architecture

```
┌─────────────┐       ┌──────────────┐       ┌─────────────────────┐
│  CLI        │──────>│  AWS Glue    │──────>│  Amazon Keyspaces   │
│  (keyspaces │       │  (Spark/     │       │  (Cassandra tables) │
│   -glue)    │       │   Scala)     │       └─────────────────────┘
└─────────────┘       └──────┬───────┘
                             │
                             v
                      ┌──────────────┐
                      │  Amazon S3   │
                      │  (data store)│
                      └──────────────┘
```

## S3 Path Convention

All export operations write to a consistent path structure:

```
s3://{bucket}/export/{keyspace}/{table}/{type}/year=YYYY/month=MM/day=DD/hour=HH/minute=mm
```

Where `{type}` is one of: `snapshot`, `incremental`, or `bulk-delete`.

## File Reference

| File | Purpose |
|------|---------|
| `keyspaces-glue` | CLI entrypoint (shell wrapper) |
| `keyspaces_glue_cli.py` | CLI application (Python) |
| `requirements.txt` | Python dependencies |
| `.keyspaces-glue.json` | Local config (created by `config` command) |
| `glue-setup-template.yaml` | Base CloudFormation template (IAM role, S3 bucket) |
| `keyspaces-application.conf` | Driver config for Amazon Keyspaces |
| `cassandra-application.conf` | Driver config for Apache Cassandra |
