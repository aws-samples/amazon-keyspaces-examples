

## Connecting to Amazon Keyspaces from SageMaker Notebook with Python


This code shows how to connect to Amazon Keyspaces from SageMaker using an [service-specific credentials](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.html) for an existing AWS Identity and Access Management (IAM) user.

Service-specific credentials aren’t the only way to authenticate and authorize access to Amazon Keyspaces resources. We recommend using the AWS authentication plugin for Cassandra drivers.

The following code is an example of a service-specific credential .

```
"ServiceSpecificCredential": {
        "CreateDate": "2019-10-09T16:12:04Z",
        "ServiceName": "cassandra.amazonaws.com",
        "ServiceUserName": "keyspace-user1-at-11122223333",
        "ServicePassword": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "ServiceSpecificCredentialId": "ACCAYFI33SINPGJEBYESF",
        "UserName": " keyspace-user1",
        "Status": "Active"
    }
}
```



### Prerequisites<a name="Prerequisites"></a></a>
This notebook was tested with conda_python3 kernel and should work with Python 3.x.

The Notebook execution role must include permissions to access Amazon Keyspaces and Assume the role.

*  To access Amazon Keyspaces database - can use AmazonKeyspacesReadOnlyAccess or AmazonKeyspacesFullAccess managed policies. Use the least privilege approach for your production application.
[AWS Identity and Access Management for Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces/latest/devguide/security-iam.html)

* To assume the role you need to have [sts:AssumeRole action](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html) permissions
    ```
    {
      "Version": "2012-10-17",  
      "Statement": [  
        {  
           "Action": [  
           "sts:AssumeRole"  
          ],  
          "Effect": "Allow",  
          "Resource": "*"  
        }
      ]
    }
    ```

#### Note:
Amazon Keyspaces is available in the following [AWS Regions](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html)


### Running the sample
*  Import Notebook into SageMaker 	
