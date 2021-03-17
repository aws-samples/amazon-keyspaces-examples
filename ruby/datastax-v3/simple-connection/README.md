## Connecting to Amazon Keyspaces with Ruby

This sample project shows the use of the DataStax Ruby Driver for Apache Cassandra using Service-Specific Credentials.

### Prerequisites

You should be running Ruby 2.7.2 or later.  


### Installing dependencies
Install the dependencies by using `bundle`.

```
$ bundle install
```

### Running the sample
You will first need to generate Service Specific Credentials as detailed in [Amazon Keyspaces Developer Guide: Generate Service-Specific Credentials](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.html#programmatic.credentials.ssc)

To run, set the following environment variables...

- SSC_USER_NAME - Service specific credential login (ex: "janeroe-at-111122223333")
- SSC_PASSWORD - Service password for SSC_USER_NAME 
- AWS_REGION - Region to connect to (ex: "us-west-2")

To execute simple execute ruby...

```
$ ruby simple-ruby-connection.rb
```






