# Load data

A simple process that creates and bulk fills a table in Keyspaces.

The table is populated with country data which can then be queried via the accompanying *query-api-server* application.

## Command-line options

```
Usage: load-data [options]

Options:
  -V, --version              output the version number
  -r, --region <region>      region-id
  -k, --keyspace <keyspace>  keyspace-id
  -t, --table <table>        table-id
  -h, --help                 display help for command
```

## Build

This application is supplied as part of a larger code sample package. For build and run instructions please refer to top-level package README.