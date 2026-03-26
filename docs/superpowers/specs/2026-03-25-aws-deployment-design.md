# Phase 2: AWS Deployment — EC2 + Docker Compose

## Overview

Deploy the existing Temporal ML pipeline to AWS using a single EC2 instance running Docker Compose. Terraform manages all AWS resources. The Python application code does not change — only environment variables differ between local and AWS.

## Architecture

```
Your Machine                           AWS (us-east-1)

  terraform apply ──────────>    VPC (10.0.0.0/16)
  ssh into EC2                   ├── Public Subnet (us-east-1a)
  docker compose up              │   └── EC2 (t3.xlarge, Docker + Compose installed)
                                 │       ├── temporal-server (port 7233)
                                 │       ├── temporal-ui (port 8080)
                                 │       ├── postgresql
                                 │       ├── default-worker
                                 │       ├── training-worker
                                 │       └── trigger
                                 │
                                 ├── S3: churn-pipeline-<random> bucket
                                 │   └── Event notification ──> SQS
                                 ├── SQS: s3-notifications queue
                                 └── Security Groups
                                     ├── SSH (22) ── your IP only
                                     └── Temporal UI (8080) ── your IP only
```

## What Changes from Local

### Stays the same
- Most Python code in `src/` — unchanged
- `Dockerfile.worker` — same image
- `docker-compose.yml` — base file unchanged (still works locally)

### Minor code change
- `src/config.py` — Change `aws_access_key_id` and `aws_secret_access_key` defaults from `"test"` to `None`. This allows boto3 to fall back to the EC2 instance role credentials on AWS, while LocalStack still works locally (override file or env vars can set them to `"test"`).

### New files
- `docker-compose.aws.yml` — override file that removes LocalStack, sets real AWS env vars
- `terraform/` — all infrastructure as code

### Differences on AWS
- No LocalStack — workers talk to real S3 and SQS
- `PIPELINE_S3_ENDPOINT_URL` and `PIPELINE_SQS_ENDPOINT_URL` set to empty (uses real AWS endpoints)
- Workers get S3/SQS access via EC2 instance IAM role (no hardcoded credentials in containers)
- S3 bucket has a random suffix for global uniqueness

## AWS Resources (Terraform)

| Resource | Purpose |
|----------|---------|
| VPC | 10.0.0.0/16 CIDR, DNS enabled |
| Public Subnet | Single subnet in us-east-1a |
| Internet Gateway | Internet access for EC2 |
| Route Table | Routes 0.0.0.0/0 through IGW |
| Security Group | Inbound: SSH (22) + Temporal UI (8080) from user's IP. Outbound: all |
| EC2 Instance | t3.xlarge, Amazon Linux 2023, Docker + Compose installed via user data script |
| Key Pair | SSH access using existing `~/.ssh/id_rsa.pub` (generate with `ssh-keygen` if missing) |
| IAM Role + Instance Profile | Attached to EC2, grants S3 and SQS access |
| S3 Bucket | `churn-pipeline-<random>` with event notification to SQS, `force_destroy = true` |
| SQS Queue | `s3-notifications`, receives S3 ObjectCreated events for `raw/` prefix |
| S3 Bucket Notification | Sends ObjectCreated events to SQS for objects with `raw/` prefix |

## Terraform Organization

```
terraform/
├── main.tf              # Provider config, data sources (AMI, caller identity)
├── variables.tf         # Input variables (region, instance type, SSH key path, allowed IP)
├── outputs.tf           # EC2 public IP, S3 bucket name, SSH command
├── vpc.tf               # VPC, subnet, IGW, route table
├── security_group.tf    # Security group rules
├── iam.tf               # IAM role, policy, instance profile
├── ec2.tf               # EC2 instance, key pair, user data
├── s3.tf                # S3 bucket, bucket notification
├── sqs.tf               # SQS queue, queue policy (allow S3 to send)
└── terraform.tfvars     # User-specific values (gitignored)
```

## Docker Compose Override

`docker-compose.aws.yml` overrides the base `docker-compose.yml`:
- Removes the `localstack` service
- Removes `localstack` dependency from workers and trigger
- Sets `PIPELINE_S3_ENDPOINT_URL` and `PIPELINE_SQS_ENDPOINT_URL` to empty (uses real AWS)
- Sets `PIPELINE_S3_BUCKET` to the Terraform-created bucket name (via `${PIPELINE_S3_BUCKET}` env var)
- Sets `PIPELINE_S3_REGION` to `us-east-1` explicitly
- Does not set AWS credentials — boto3 picks them up from the EC2 instance metadata service (Docker's default bridge network can reach `169.254.169.254`)

**How the bucket name gets into the compose file:** Terraform outputs the bucket name. Before running `docker compose`, the user exports it:
```bash
export PIPELINE_S3_BUCKET=$(terraform -chdir=/path/to/terraform output -raw s3_bucket_name)
docker compose -f docker-compose.yml -f docker-compose.aws.yml up --build -d
```

The `docker-compose.aws.yml` references `${PIPELINE_S3_BUCKET}` which Docker Compose interpolates from the shell environment.

## EC2 User Data (Bootstrap)

The EC2 instance is bootstrapped via user data script that:
1. Installs Docker and Docker Compose plugin
2. Starts Docker service
3. Adds `ec2-user` to docker group

After SSH, the user clones the repo and runs docker compose manually.

## IAM Permissions

The EC2 instance role needs:
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the pipeline bucket
- `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueUrl`, `sqs:GetQueueAttributes` on the notifications queue

## Security

- SSH and Temporal UI restricted to user's IP via security group
- No TLS (accepted risk for learning project)
- Temporal server (7233) not exposed externally — only accessible within EC2
- PostgreSQL not exposed externally — only accessible within EC2
- IAM role follows least-privilege for S3/SQS access

## Deployment Workflow

1. Configure AWS CLI: `aws configure`
2. `cd terraform && terraform init && terraform apply`
3. SSH into EC2: `ssh -i <key> ec2-user@<public-ip>`
4. Clone repo: `git clone <repo-url>` or `scp` files
5. Build and start: `docker compose -f docker-compose.yml -f docker-compose.aws.yml up --build -d`
6. Upload CSV: `aws s3 cp data/sample_churn.csv s3://<bucket>/raw/sample_churn.csv`
7. Monitor: open `http://<ec2-public-ip>:8080`

## Teardown

```bash
terraform destroy
```

This removes all AWS resources including the S3 bucket (configured with `force_destroy = true` so it deletes even when non-empty). Remember to run this when done to avoid ongoing charges.

To save money without destroying everything, you can stop the EC2 instance:
```bash
aws ec2 stop-instances --instance-ids <instance-id>
```
Stopped instances incur no compute charges (only EBS storage, ~$0.80/month for 8 GB).

## Cost Estimate

| Resource | Monthly Cost |
|----------|-------------|
| EC2 t3.xlarge | ~$120 |
| S3 | < $1 |
| SQS | < $1 |
| VPC/IGW | $0 |
| **Total** | **~$122/month** |

Run `terraform destroy` when done, or stop the EC2 instance between sessions. For short experiments (a few hours), cost is negligible.
