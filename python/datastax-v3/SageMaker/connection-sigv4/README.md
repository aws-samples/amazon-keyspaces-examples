## Connecting to Amazon Keyspaces from SageMaker Notebook with Python

This code shows how to connect to Amazon Keyspaces from SageMaker using an authentication plugin for temporary credentials. This plugin enables IAM users, roles, and federated identities to add authentication information to Amazon Keyspaces API requests using the [AWS Signature Version 4 process (SigV4)](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html) 

In this example, we do NOT need to generate Keyspaces service-specific credentials.



### Prerequisites

The Notebook execution role must include permissions to access Amazon Keyspaces and Assume the role.

*  To access Amazon Keyspaces database - use AmazonKeyspacesReadOnlyAccess or AmazonKeyspacesFullAccess managed policies. Use the _least privileged approach_ for your production application.  
See more at
[AWS Identity and Access Management for Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces/latest/devguide/security-iam.html).

* To assume the role, you need to have [sts:AssumeRole action](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html) permissions.
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
Amazon Keyspaces is available in the following [AWS Regions](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html).

This notebook was tested with conda_python3 kernel and should work with Python 3.x.



### Running the sample
*  Import Notebook into SageMaker 	
