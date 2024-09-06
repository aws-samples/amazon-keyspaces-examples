## Using Amazon Elasticache with Redis OSS compatibility as a cache for Amazon Keyspaces

This sample project shows the use of the Amazon Elasticache Redis as a cache for book awards data stored in Amazon Keyspaces. The sample uses  DataStax Python Driver for Apache Cassandra to connect to Amazon Keyspaces, and Redis Client to connect to Amazon Elasticache Redis. SigV4 has been used for authentication using short-term credentials.

### Prerequisites
You should have Python and pip installed.  This sample works with Python 3.x.

You should also setup Amazon Keyspaces with an IAM user.  See [Accessing Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces/latest/devguide/accessing.html) for more.

You need the Starfield digital certificate 'sf-class2-root.crt' downloaded locally or in your home directory - provide the certificate path on line 13 of write_through_caching_sample/ks_redis.py. The certificate can be found at write_through_caching_sample/resources.

You should have the connection string for your Amazon Elasticache Redis cluster which you will provide on line  of write_through_caching_sample/ks_redis.py.

You should have the keyspace name and table name for your Amazon Keyspaces resource which you will provide on line 36 and line 37  of write_through_caching_sample/ks_redis.py respectively.

Sample data can be found at write_through_caching_sample/resources/ and can be loaded using [CQLSH](https://docs.aws.amazon.com/keyspaces/latest/devguide/bulk-upload.html) or [DSBulk](https://docs.aws.amazon.com/keyspaces/latest/devguide/dsbulk-upload.html).


### Running the sample

This sample uses Boto3 which will find credentials based on environment variables, config or credentials file on your client, see [Boto3 Credentials Guide](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for how to set this up.  

You can quickly get this to run by explicitly setting the following environment variables...

- AWS_REGION  (for example 'us-east-1)
- AWS_ACCESS_KEY_ID  (ex: 'AKIAIOSFODNN7EXAMPLE')
- AWS_SECRET_ACCESS_KEY (ex: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

### Install the dependencies 

From the project directory... 
```
pip install -r requirements.txt
```

### Testing
From this project directory...
```
python3 write_through_caching_sample/test_functions.py
```
