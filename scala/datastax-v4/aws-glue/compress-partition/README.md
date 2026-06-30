## Compress Partition

This example provides a Scala script for reading rows from a source table, grouping them by partition key (or a clustering column prefix), compressing the grouped rows as JSON into a blob, and writing the result to a target table.

### Use Case

Wide partitions with many clustering rows can be expensive to store and read. This script allows you to:
- Collapse many rows into fewer rows by grouping and compressing
- Choose which clustering column level to group by
- Select from ZSTD (default), LZ4, SNAPPY, or GZIP compression
- Filter source data with a where clause (e.g., compress only data older than a date)

### Prerequisites

* Run `./keyspaces-bulk-cli bootstrap` from the [parent directory](../) to set up infrastructure and deploy all Glue jobs
* The target table must already exist with the correct schema

### How It Works

Given a source table:
```cql
CREATE TABLE mykeyspace.events (
    tenant_id text,
    event_id text,
    payload text,
    PRIMARY KEY ((tenant_id), event_id)
);
```

The compressed target table:
```cql
CREATE TABLE mykeyspace.events_compressed (
    tenant_id text,
    data blob,
    PRIMARY KEY ((tenant_id))
);
```

All rows sharing a partition key are serialized to JSON, compressed, and stored as a single blob row.

### Running Compress Partition

Partition-level compression:
```bash
./keyspaces-bulk-cli compress-partition \
  --keyspace mykeyspace \
  --source-table events \
  --target-table events_compressed \
  --compression ZSTD
```

Group by a clustering column:
```bash
./keyspaces-bulk-cli compress-partition \
  --keyspace mykeyspace \
  --source-table tableA \
  --target-table tableB \
  --compression ZSTD \
  --group-by-column date
```

Compress only older data:
```bash
./keyspaces-bulk-cli compress-partition \
  --keyspace mykeyspace \
  --source-table events \
  --target-table events_compressed \
  --compression ZSTD \
  --where-clause "timestamp < '2026-01-01'"
```

### Script Arguments

| Argument | Description | Default |
| :--- | :--- | :--- |
| --KEYSPACE_NAME | Keyspace containing both tables | - |
| --SOURCE_TABLE | Source table to read from | - |
| --TARGET_TABLE | Target table to write compressed data to | - |
| --COMPRESSION | Algorithm: ZSTD, LZ4, SNAPPY, or GZIP | ZSTD |
| --GROUP_BY_COLUMN | Clustering column to group by. Omit for partition-level grouping | (empty) |
| --WHERE_CLAUSE | Optional filter condition on source data | (none) |
| --DRIVER_CONF | Driver configuration file | keyspaces-application.conf |

### Compression Algorithms

| Algorithm | Characteristics |
| :--- | :--- |
| ZSTD | Best compression ratio. Good speed. Default choice. |
| LZ4 | Fastest compression/decompression. Lower ratio. |
| SNAPPY | Balanced speed and ratio. Widely used in big data. |
| GZIP | Highest ratio for text. Slower than ZSTD. |

### Decompression

The compressed blob stores JSON data. To decompress on the client side:

**ZSTD (Java)**
```java
byte[] decompressed = com.github.luben.zstd.Zstd.decompress(blob, originalSize);
String json = new String(decompressed, StandardCharsets.UTF_8);
```

**LZ4 (Java)**
```java
ByteBuffer buf = ByteBuffer.wrap(blob);
int originalSize = buf.getInt();
byte[] compressed = new byte[blob.length - 4];
System.arraycopy(blob, 4, compressed, 0, compressed.length);
byte[] decompressed = LZ4Factory.fastestInstance().fastDecompressor().decompress(compressed, originalSize);
String json = new String(decompressed, StandardCharsets.UTF_8);
```

**SNAPPY (Java)**
```java
byte[] decompressed = org.xerial.snappy.Snappy.uncompress(blob);
String json = new String(decompressed, StandardCharsets.UTF_8);
```

**GZIP (Java)**
```java
GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(blob));
byte[] decompressed = gis.readAllBytes();
String json = new String(decompressed, StandardCharsets.UTF_8);
```

### Notes

- The target table schema must match: same partition keys as source, clustering columns up to (and including) the GROUP_BY_COLUMN, plus a `data` column of type `blob`
- Amazon Keyspaces has a 1 MB row size limit. If compressed data exceeds this, group at a finer clustering column level
- The JSON format preserves column names and types for straightforward deserialization
