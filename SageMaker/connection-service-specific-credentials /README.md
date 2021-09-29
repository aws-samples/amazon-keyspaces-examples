

## Connecting to Amazon Keyspaces from SageMaker Notebook with Python


This code shows how to connect to Amazon Keyspaces from SageMaker using [service-specific credentials](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.html).

Service-specific credentials aren’t the only way to authenticate and authorize access to Amazon Keyspaces resources. We recommend using AWS authentication plugin for Cassandra drivers .


### Prerequisites<a name="Prerequisites"></a></a>
The Notebook execution role must include permissions to access Amazon Keyspaces and [Secret Manager](https://aws.amazon.com/secrets-manager/).

*  To access Amazon Keyspaces database - use AmazonKeyspacesReadOnlyAccess or AmazonKeyspacesFullAccess managed policies. Use the _least privileged approach_ for your production application.  
See more at
[AWS Identity and Access Management for Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces/latest/devguide/security-iam.html).

* To use AWS Secret Manager, the Notebooks execution role must include  [SecretsManagerReadWrite](https://docs.aws.amazon.com/secretsmanager/latest/userguide/reference_available-policies.html) managed policy.



### Security Configuration

1. Generate [Keyspaces Service-Specific Credentials](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.html)


        Example of a service-specific credential

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

2. Store ServiceUserName and ServicePassword in the SecretManager.  As a best practice, we don't want to store credentials as a plain text in the SageMaker Notebooks.

  In this example I'm using
_Keyspaces_Server_Generated_credential_ as a Secret Name and _keyspaces_generated_id_ and _keyspaces_generated_pw_ fields to store Keyspaces ID and password.




#### Note:
Amazon Keyspaces is available in the following [AWS Regions](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html).

This notebook was tested with conda_python3 kernel and should work with Python 3.x.

### Running the sample
*  Import Notebook into SageMaker 	
