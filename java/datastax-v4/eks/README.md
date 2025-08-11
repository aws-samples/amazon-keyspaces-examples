# Sample Application for Amazon EKS to Amazon Keyspaces connectivity Testing using SigV4 Authentication

This example showcases a Spring Boot application designed for testing the connection between Amazon Elastic Kubernetes Service (EKS) and Amazon Keyspaces, utilizing SigV4 authentication for secure connectivity.

## Prerequisites

- Amazon EKS cluster
- Amazon Keyspaces
- AWS CLI
- kubectl
- Docker
- Amazon ECR
- IAM Role, policies and ServiceAccount 

You need to finish pre-requisites including IAM Role, Policies and Service Account before moving to setup.

## Setup

### Create Keyspace and Table in Amazon Keyspaces

```
CREATE KEYSPACE aws WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;
CREATE TABLE aws.user (
    username text PRIMARY KEY,
    fname text,
    last_update_date timestamp,
    lname text
);
```

### Clone the Repository

```shell
git clone https://github.com/aws-samples/amazon-keyspaces-examples.git
```

### Build from Source code (optional)
In this example, Application code uses ```us-east-1``` AWS Region and ```cassandra.us-east-1.amazonaws.com``` as Amazon Keyspaces Endpoint. If you want to build your own jar from source code to customize, you can clone the repo, make custom changes and build jar using maven before moving to next step.

### Create your Amazon ECR repository
```
aws ecr create-repository --repository-name <my repository>
```

## Build Image and Push to Container Registry

### Build Docker Image
Navigate to the base Directory, Modify the Dockerfile as needed and build the Docker Image. Replace build tag with your AWS Account ID, AWS Region where ECR repository created.

```
cd amazon-keyspaces-examples/java/datastax-v4/eks
docker build -t <aws_account_id>.dkr.ecr.<AWS_region>.amazonaws.com/mykeyspacessigv4springbootapp:latest .
```

### Retrieve Authentication Token to push the image to ECR
```
aws ecr get-login-password --region <AWS_region> | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.<AWS_region>.amazonaws.com
```

### Push Docker Image to Amazon ECR repository
```
docker push <aws_account_id>.dkr.ecr.<AWS_region>.amazonaws.com/mykeyspacessigv4springbootapp:latest .
```
## Run your Application in EKS


### EKS Deployment configuration
Modify deployment.yaml using your custom configuration information. Some of the important properties to change in deployment.yaml are ```EKS namespace```,```serviceAccountName```,```image```,```AWS_ROLE_ARN value```

### Deploy Application to Amazon EKS
```
kubectl apply -f deployment.yaml
```

## Check deployment and POD status
```
kubectl get deployments -n <your namespace>
kubectl get pods -n <your namespace>
```

### Validate Connectivity

Validate connectivity by logging into CQL Editor or cqlsh to check the record in Amazon Keyspaces table aws.user

```
select * from aws.user;
```

Example output
```
 username | fname  | last_update_date                | lname
----------+--------+---------------------------------+-------
     test | random | 2023-11-16 16:22:01.094000+0000 |     k
```

### (Optional) verify logs
You can also verify logs by running below command

```
kubectl logs -f <POD_name> -n <your namespace>
```

