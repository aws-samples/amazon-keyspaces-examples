# ZDM Proxy — Cassandra to Amazon Keyspaces Migration

## How to Use This Skill

Activate this skill when you need to deploy, operate, troubleshoot, or modify the ZDM Proxy solution for migrating Apache Cassandra to Amazon Keyspaces. It provides ready-to-use commands, parameter references, and solutions to common issues so you can work with the solution without searching through multiple files.

Use cases:
- Deploying the ZDM proxy stack for the first time
- Rebuilding and pushing the Docker image after changes
- Verifying deployment health (ECS tasks, NLB targets)
- Running dual-write tests to confirm data replication
- Troubleshooting connectivity or authentication failures
- Tearing down and redeploying the stack

## Overview

The ZDM Proxy intercepts Cassandra client connections, forwards reads to the origin cluster, and dual-writes to both origin and Amazon Keyspaces. It runs on ECS Fargate behind an internal NLB, with socat handling TLS to Keyspaces inside each container.

## Key Resources

| Resource     | Value                                                                                          |
| ------------ | ---------------------------------------------------------------------------------------------- |
| GitHub Repo  | https://github.com/aws-samples/amazon-keyspaces-examples/tree/main/migration/online/zdm-proxy |
| Local Path   | `amazon-keyspaces-examples/migration/online/zdm-proxy/`                                        |
| CFN Template | `zdm-proxy-cloudformation.yaml`                                                               |
| Dockerfile   | Extends `datastax/zdm-proxy:latest` with socat TLS + openssl                                  |
| Entrypoint   | `entrypoint.sh` — DNS resolution, socat TLS, topology awareness                               |
| Proxy Port   | 14002 (NLB + ECS tasks)                                                                       |
| Target Port  | 9142 (socat inside container, TLS to Keyspaces)                                                |
| Origin Port  | 9042 (Cassandra native transport)                                                              |

## Build and Push Image

```bash
# Build (use --platform linux/amd64 on Apple Silicon)
finch build --platform linux/amd64 -t zdm-proxy .
# Or: docker build --platform linux/amd64 -t zdm-proxy .

# Tag and push
finch tag zdm-proxy:latest <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/zdm-proxy:latest
finch push <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/zdm-proxy:latest
```

## Deploy Stack

```bash
aws cloudformation create-stack \
  --stack-name zdm-proxy \
  --template-body file://zdm-proxy-cloudformation.yaml \
  --capabilities CAPABILITY_IAM \
  --region us-east-1 \
  --parameters \
    ParameterKey=VPCId,ParameterValue=<VPC_ID> \
    ParameterKey=PrivateSubnetIds,ParameterValue=\"<SUBNET1>,<SUBNET2>,<SUBNET3>\" \
    ParameterKey=SecurityGroupId,ParameterValue=<SG_ID> \
    ParameterKey=RouteTableId,ParameterValue=<RTB_ID> \
    ParameterKey=ZDMOriginContactPoints,ParameterValue=\"<CASSANDRA_IPS>\" \
    ParameterKey=ZDMOriginPort,ParameterValue=9042 \
    ParameterKey=ZDMOriginUsername,ParameterValue=<ORIGIN_USER> \
    ParameterKey=ZDMOriginPassword,ParameterValue=<ORIGIN_PASS> \
    ParameterKey=ZDMTargetUsername,ParameterValue=<KEYSPACES_USER> \
    ParameterKey=ZDMTargetPassword,ParameterValue=<KEYSPACES_PASS> \
    ParameterKey=ServiceReplicaCount,ParameterValue=3 \
    ParameterKey=ZDMProxyPort,ParameterValue=14002 \
    ParameterKey=ECRImage,ParameterValue=<ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/zdm-proxy:latest
```

## Verify Deployment

```bash
# Stack status
aws cloudformation describe-stacks --stack-name zdm-proxy \
  --query 'Stacks[0].{Status:StackStatus,NLB:Outputs[?OutputKey==`NetworkLoadBalancerDNS`].OutputValue|[0]}'

# ECS service health
aws ecs describe-services --cluster <CLUSTER> --services <SERVICE> \
  --query 'services[0].{Running:runningCount,Desired:desiredCount}'

# NLB target health
aws elbv2 describe-target-health --target-group-arn <TG_ARN> \
  --query 'TargetHealthDescriptions[*].{IP:Target.Id,State:TargetHealth.State}'
```

## Dual-Write Test

1. Connect to the proxy using Keyspaces credentials (not Cassandra credentials)
2. Set consistency to `LOCAL_QUORUM` (required by Keyspaces)
3. INSERT a test row
4. Verify on Cassandra directly (origin)
5. Verify on Keyspaces directly (target)

```cql
CONSISTENCY LOCAL_QUORUM;
INSERT INTO test_keyspace.users (user_id, name, email, created_at)
  VALUES (uuid(), 'Test User', 'test@example.com', toTimestamp(now()));
```

## Common Gotchas

| Issue                          | Cause                                | Fix                                       |
| ------------------------------ | ------------------------------------ | ----------------------------------------- |
| Consistency level ONE rejected | Keyspaces only supports LOCAL_QUORUM | Set `CONSISTENCY LOCAL_QUORUM`            |
| Auth failure on connect        | Client must use Keyspaces creds      | Connect with target credentials           |
| Target connection timeout      | Host assignment bypasses socat       | `ZDM_TARGET_ENABLE_HOST_ASSIGNMENT=false` |
| Double TLS failure             | TLS enabled on proxy + socat         | `ZDM_TARGET_TLS_ENABLED=false`            |
| Can't pull secrets             | Missing Secrets Manager VPCE         | Ensure VPCE for secretsmanager exists     |
| Port 9142 conflict             | Another process binding 9142         | Don't set ZDMProxyPort to 9142            |

## Key Environment Variables

| Variable                                   | Value       | Why                                  |
| ------------------------------------------ | ----------- | ------------------------------------ |
| `ZDM_TARGET_CONTACT_POINTS`                | `localhost` | Socat forwards to Keyspaces          |
| `ZDM_TARGET_PORT`                          | `9142`      | Socat listens here                   |
| `ZDM_TARGET_TLS_ENABLED`                   | `false`     | Socat handles TLS, not the proxy     |
| `ZDM_TARGET_ENABLE_HOST_ASSIGNMENT`        | `false`     | Force traffic through socat          |
| `ZDM_FORWARD_CLIENT_CREDENTIALS_TO_ORIGIN` | `true`      | Proxy uses configured origin creds   |

## Stack Resources Created

- ECS Fargate cluster, task definition, and service
- Internal NLB with TCP listener and target group
- 7 VPC endpoints (Keyspaces, ECR API, ECR DKR, STS, CW Logs, Secrets Manager, S3)
- 2 Secrets Manager secrets (origin + target credentials)
- IAM execution role (ECR pull, CW Logs, Secrets Manager)
- CloudWatch Logs log group (30-day retention)

## Tear Down

```bash
aws cloudformation delete-stack --stack-name zdm-proxy --region us-east-1
aws cloudformation wait stack-delete-complete --stack-name zdm-proxy --region us-east-1
```
