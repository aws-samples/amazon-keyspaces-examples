## Using SigV4 Scala Example
This example showcases how to use SigV4 plugin with Scala to connect to Amazon Keyspaces, using
the DataStax V4 driver.

### Prerequisites
For connecting via SigV4 and role based authentication you will want to set the following environment
variables (or set a default profile), see [Using Credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html) for more information
```
    AWS_SECRET_ACCESS_KEY
    AWS_ACCESS_KEY_ID
```

Under `./resources/application.conf` you will find the configuration used to connect to Amazon Keyspaces.  Use this to change the details of your connection. 

### Running the example.

You can run this from Intellij by `New Project from existing Sources` and selecting build.sbt and accepting the defaults.

If you wish to run the example from the command line simply run sbt at the command line. 

```
$ sbt run 
```  


