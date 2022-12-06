# Query API

A simple web server that offers a REST API that can be used to query a table
in Keyspaces.

The table is assumed to be populated with country data (using the *load-data* application).

## Resource paths

The server supports the following resources paths:
- *ping*: simple HTTP ping that can be used as a health check
- */countries*: lists all countries in the table (in JSON)
- */countries/xx*: shows details for country whose country code is *xx* (in JSON)

The optional query parameter *?pretty* can be used to prettify the JSON output.

Examples (assuming service running behind an ALB)

Details for country *India*:
```
http://demo-alb-70374535.eu-west-1.elb.amazonaws.com/countries/in?pretty
```

List all countries:
```
http://demo-alb-70374535.eu-west-1.elb.amazonaws.com/countries?pretty
```

## Command-line options

```
Usage: query-api-server [options]

Options:
  -V, --version              output the version number
  -p, --port <port>          server port (default: "80")
  -r, --region <region>      region-id
  -k, --keyspace <keyspace>  keyspace-id
  -t, --table <table>        table-id
  -h, --help                 display help for command
```


## Build

This application is supplied as part of a larger code sample package. For build and run instructions please refer to top-level package README.