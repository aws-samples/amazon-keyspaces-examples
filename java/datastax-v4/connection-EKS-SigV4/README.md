# Connection to Amazon Keyspaces from Amazon EKS using SigV4 Authentication

This example demonstrates how to connect to Amazon Keyspaces from an Amazon EKS cluster using SigV4 authentication.

## Prerequisites

- Amazon EKS cluster
- Amazon Keyspaces
- AWS CLI
- kubectl
- Docker
- Amazon ECR

## Setup

### Clone the Repository

```shell
git clone https://github.com/naveenkantamneni/amazon-keyspaces-examples.git
```

## Create Keyspace and Table in Amazon Keyspaces

```
CREATE KEYSPACE aws WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;
CREATE TABLE aws.user (
    username text PRIMARY KEY,
    fname text,
    last_update_date timestamp,
    lname text
);
```

## Create your Amazon EKS Cluster by following steps in below page
https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html

## Create an OIDC provider for your EKS cluster by following instructions on below page
https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html

## Create a Kubernetes Service Account to assume an IAM role by following the instructions on below page. Follow Instructions under AWS CLI section and make sure to add IAM Policy "AmazonKeyspacesFullAccess" to your IAM Role in step "g"
https://docs.aws.amazon.com/eks/latest/userguide/associate-service-account-role.html

## Confirm that the "AmazonKeyspacesFullAccess" policy that you attached to your role in a previous step is attached to the role.

```
aws iam list-attached-role-policies --role-name myeks-servicerole-assumewebid
```

An example output is as follows

```
{
    "AttachedPolicies": [
        {
            "PolicyName": "AmazonKeyspacesFullAccess",
            "PolicyArn": "arn:aws:iam::aws:policy/AmazonKeyspacesFullAccess"
        }
    ]
}
```

## Create your Amazon ECR repository
```
aws ecr create-repository --repository-name <my repository>
```

## Navigate to the base Directory, Modify the Dockerfile as needed and build the Docker Image. Make sure to use your AWS Account ID, AWS Region where ECR repository created, ECR repository name and your tag during build

```
cd amazon-keyspaces-examples/java/datastax-v4/connection-EKS-SigV4
docker build -t <aws_account_id>.dkr.ecr.<AWS_region>.amazonaws.com/<ECR_repositoryname>:<your_tag> .
```

## Retrieve Authentication Token to push the image to ECR
```
aws ecr get-login-password --region <AWS_region> | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.<AWS_region>.amazonaws.com
```

## Push Docker Image to Amazon ECR repository
```
docker push <aws_account_id>.dkr.ecr.<AWS_region>.amazonaws.com/<ECR_repositoryname>:<your_tag>
```

## Modify deployment.yaml with relevant information and Deploy Application to Amazon EKS
```
kubectl apply -f deployment.yaml
```

## Check the POD status
```
kubectl get pods -n <your namespace>
```

### Optional, Validate Logs to ensure connectivity to Amazon Keyspaces
```
kubectl logs -f <POD_name> -n <your namespace>
```

