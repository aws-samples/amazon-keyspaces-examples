# ZDM Dual Write Proxy for Amazon Keyspaces Migration

This project extends the official [ZDM Proxy](https://github.com/datastax/zdm-proxy) to support seamless **zero-downtime migration** from **Apache Cassandra** to **Amazon Keyspaces (for Apache Cassandra)** with AWS best practices.

The ZDM Proxy features:
- Used to perform online migration from one Cassandra cluster to another.
- Can perform dual writes without refactoring existing applications.
- Perform dual reads for query validation.

This repository introduces key enhancements for use with Amazon Web Services and Amazon Keyspaces:

- A **Dockerfile** and build script to create a custom ZDM Proxy image with socat TLS support, which you build and push to your own **Amazon ECR** repository.
- A **CloudFormation template** to deploy the proxy on **AWS Fargate**, ensuring a scalable, serverless, and secure setup within your existing AWS infrastructure.
- Credentials stored in **AWS Secrets Manager** and injected into ECS tasks at runtime.
- **VPC endpoints** for private connectivity to Keyspaces, ECR, STS, CloudWatch Logs, Secrets Manager, and S3.
- Customization to support Keyspaces system tables.

The proxy is deployed with Amazon ECS on Fargate which can scale up and down based on application demand. An internal Network Load Balancer distributes client traffic across the ECS tasks and provides a stable endpoint for application connectivity.

![this screenshot](aws-ecs-zdm.drawio.svg)

## Project Structure

| File                            | Description                                                                                                                  |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `Dockerfile`                    | Builds the custom ZDM Proxy image with socat TLS support and the Keyspaces root certificate.                    |
| `entrypoint.sh`                 | Container entrypoint that configures networking for Keyspaces connectivity and launches the ZDM proxy. |
| `move-docker-to-ecr.sh`         | Automates Docker image build, tagging, and pushing to Amazon ECR. Also downloads the required TLS root cert.                 |
| `zdm-proxy-cloudformation.yaml` | CloudFormation template for deploying the proxy on ECS Fargate behind an internal NLB.                            |

---

## Prerequisites

- An existing VPC with private subnets where the ZDM proxy will be deployed. The origin Cassandra cluster must be reachable from these subnets.
- An Amazon ECR repository to host the custom ZDM proxy image. The image is built from the included Dockerfile and pushed to your own ECR repository.
- Keyspaces service-specific credentials for the target. These are tied to an IAM user and can be created via the console or CLI (`aws iam create-service-specific-credential --user-name <IAM_USER> --service-name cassandra.amazonaws.com`). See [Create service-specific credentials](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.ssc.html).
- The template creates VPC endpoints for Keyspaces, ECR, STS, CloudWatch Logs, Secrets Manager, and S3. If your VPC already has any of these endpoints, remove the corresponding `AWS::EC2::VPCEndpoint` resources from the template before deploying to avoid conflicts.

---

## Parameters (CloudFormation Template)

### Network Configuration

- **VPCId**: ID of your target VPC.
- **PrivateSubnetIds**: List of private subnet IDs.
- **SecurityGroupId**: Security group for the NLB, VPC endpoints, and ECS tasks.
- **RouteTableId**: Route table associated with private subnets (used for the S3 gateway endpoint).

### Origin Cassandra Configuration

- **ZDMOriginContactPoints**: Comma-separated IP addresses or DNS names for the origin Cassandra cluster.
- **ZDMOriginPort**: Native transport port for the origin cluster. Default is `9042`.
- **ZDMOriginUsername / ZDMOriginPassword**: Auth credentials for the origin cluster. Stored in Secrets Manager at deploy time.

### Target Keyspaces Configuration

- **ZDMTargetUsername / ZDMTargetPassword**: Keyspaces service-specific credentials. Stored in Secrets Manager at deploy time. See [Create service-specific credentials](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.ssc.html).

> **Note:** `ZDM_TARGET_CONTACT_POINTS` and `ZDM_TARGET_PORT` are hardcoded to `localhost` and `9142` respectively. The entrypoint script starts a socat TLS proxy inside each container that forwards traffic from localhost:9142 to the Keyspaces VPC endpoint. These values should not be changed.

### Proxy Configuration

- **ServiceReplicaCount**: Number of ECS tasks to launch. Default is `3`.
- **ZDMProxyPort**: Port for the proxy service and Network Load Balancer. Default is `14002`. Do not use 9142 (reserved by socat inside the container for Keyspaces TLS forwarding).
- **ECRImage**: Full ECR image URI for the custom ZDM proxy image (built and pushed to your own ECR repository in step 1).

---

## Deployment Instructions

### 1. Build and Push Image to Amazon ECR

Build and push the custom ZDM proxy image to your ECR repository using the provided script:

```bash
./move-docker-to-ecr.sh
```

### 2. Launch CloudFormation Stack

Deploy the `zdm-proxy-cloudformation.yaml` template via the AWS Console or CLI. Provide the required parameters (VPC, subnets, security group, credentials, ECR image URI).

The stack creates the following resources:
- ECS Fargate cluster, task definition, and service
- Internal Network Load Balancer with TCP listener and target group
- 7 VPC endpoints (Keyspaces, ECR API, ECR DKR, STS, CloudWatch Logs, Secrets Manager, S3)
- 2 Secrets Manager secrets (origin and target credentials)
- IAM execution role with scoped permissions (ECR pull, CloudWatch Logs, Secrets Manager)
- CloudWatch Logs log group with 30-day retention

---

## Security and TLS

- TLS to Amazon Keyspaces is handled by socat inside each container. The socat process performs TLS termination with certificate verification and SNI, forwarding traffic from localhost:9142 to the Keyspaces VPC endpoint.
- Credentials are stored in AWS Secrets Manager and injected into ECS tasks via the `Secrets` block at runtime.
- The IAM execution role's `secretsmanager:GetSecretValue` permission is scoped to the two specific secret ARNs created by the stack.
- If using TLS for self-managed Cassandra, include the certificate in the Dockerfile.

---

## Key Environment Variables

The following environment variables are set automatically by the CloudFormation template:

| Variable | Value | Description |
| --- | --- | --- |
| `ZDM_TARGET_CONTACT_POINTS` | `localhost` | Socat handles forwarding to Keyspaces |
| `ZDM_TARGET_PORT` | `9142` | Socat listens on this port inside the container |
| `ZDM_TARGET_TLS_ENABLED` | `false` | Socat handles TLS; the proxy connects to socat in plain TCP |
| `ZDM_TARGET_ENABLE_HOST_ASSIGNMENT` | `false` | Forces all target traffic through socat (prevents bypassing) |
| `ZDM_FORWARD_CLIENT_CREDENTIALS_TO_ORIGIN` | `true` | Proxy uses configured origin credentials for Cassandra |
| `AWS_NLB_DNS` | NLB DNS name | Used by entrypoint.sh for topology awareness |

---

## Testing and Validation

Once deployed:

- Point your application to the NLB DNS name output by the CloudFormation stack on port 14002.
- Authenticate with Keyspaces service-specific credentials (the proxy forwards these to Keyspaces and uses the configured origin credentials for Cassandra).
- Use `LOCAL_QUORUM` consistency level (required by Amazon Keyspaces).
- Test dual writes by verifying data in both origin and target clusters.

---

## References

- [Amazon Keyspaces Developer Guide](https://docs.aws.amazon.com/keyspaces/latest/devguide/)
- [Official ZDM Proxy Repo](https://github.com/datastax/zdm-proxy)
