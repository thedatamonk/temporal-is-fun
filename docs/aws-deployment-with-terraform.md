# AWS Deployment with Terraform

This document details how the Temporal Churn Prediction Pipeline is deployed on AWS using Terraform.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS (us-east-1)                          │
│                                                                 │
│  ┌───────────────── VPC (10.0.0.0/16) ───────────────────────┐  │
│  │                                                            │  │
│  │  ┌──────────── Public Subnet (10.0.1.0/24) ────────────┐  │  │
│  │  │                                                      │  │  │
│  │  │   ┌─────────── EC2 (t3.xlarge) ──────────────────┐   │  │  │
│  │  │   │                                              │   │  │  │
│  │  │   │  Docker Compose runs:                        │   │  │  │
│  │  │   │  ┌──────────────────────────────────────┐    │   │  │  │
│  │  │   │  │  PostgreSQL (Temporal persistence)   │    │   │  │  │
│  │  │   │  │  Temporal Server (port 7233)         │    │   │  │  │
│  │  │   │  │  Temporal UI (port 8080) ◄───────────┼────┼───┼──┼──┼── Your Browser
│  │  │   │  │  Default Worker (default-queue)      │    │   │  │  │
│  │  │   │  │  Training Worker (training-queue)    │    │   │  │  │
│  │  │   │  │  Trigger (SQS poller)                │    │   │  │  │
│  │  │   │  └──────────────────────────────────────┘    │   │  │  │
│  │  │   │                                              │   │  │  │
│  │  │   │  IAM Role: S3 + SQS access                  │   │  │  │
│  │  │   └──────────────────────────────────────────────┘   │  │  │
│  │  │                                                      │  │  │
│  │  └──────────────────────────────────────────────────────┘  │  │
│  │                          │                                 │  │
│  │                    Internet Gateway                        │  │
│  └──────────────────────────┼─────────────────────────────────┘  │
│                             │                                    │
│  ┌──────────┐    ┌─────────┴──────────┐                         │
│  │ S3 Bucket│───>│ SQS Queue          │                         │
│  │ (data)   │    │ (S3 notifications) │                         │
│  └──────────┘    └────────────────────┘                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

When a CSV is uploaded to the S3 bucket's `raw/` prefix, S3 sends a notification to SQS. The trigger service (running inside Docker on the EC2 instance) polls SQS, picks up the event, and starts a Temporal workflow that orchestrates the full ML pipeline.

---

## Terraform Resources

All Terraform files live in `terraform/`. The deployment creates **10 AWS resources** across networking, compute, storage, messaging, and IAM.

### Provider Configuration (`main.tf`)

```hcl
terraform {
  required_version = ">= 1.0"
  required_providers {
    aws    = { source = "hashicorp/aws",    version = "~> 5.0" }
    random = { source = "hashicorp/random", version = "~> 3.0" }
  }
}
```

- **AWS provider** for all infrastructure
- **Random provider** to generate a unique suffix for the S3 bucket name (ensuring global uniqueness)
- Region configured via `var.region` (default: `us-east-1`)

### Input Variables (`variables.tf`)

| Variable | Default | Description |
|---|---|---|
| `region` | `us-east-1` | AWS region for all resources |
| `instance_type` | `t3.xlarge` | EC2 instance size (4 vCPUs, 16GB RAM) |
| `ssh_public_key_path` | `~/.ssh/id_rsa.pub` | Path to your SSH public key |
| `allowed_ip` | **Required** | Your IP in CIDR notation (e.g., `1.2.3.4/32`) |
| `project_name` | `churn-pipeline` | Prefix for all resource names |

`allowed_ip` is the only required variable -- it restricts SSH and Temporal UI access to your IP only.

---

### Networking (`vpc.tf`)

Terraform creates a minimal public network:

| Resource | Configuration | Purpose |
|---|---|---|
| **VPC** | `10.0.0.0/16` (65,536 IPs) | Isolated network for the project |
| **Public Subnet** | `10.0.1.0/24` in `{region}a` | Hosts the EC2 instance |
| **Internet Gateway** | Attached to VPC | Allows outbound internet access |
| **Route Table** | `0.0.0.0/0 → IGW` | Routes all non-local traffic to the internet |
| **Route Table Association** | Links subnet to route table | Makes the subnet public |

The subnet has `map_public_ip_on_launch = true`, so any EC2 instance launched in it gets a public IP automatically.

### Security Group (`security_group.tf`)

The EC2 security group acts as a firewall with two inbound rules:

| Direction | Port | Protocol | Source | Purpose |
|---|---|---|---|---|
| **Ingress** | 22 | TCP | `var.allowed_ip` | SSH access |
| **Ingress** | 8080 | TCP | `var.allowed_ip` | Temporal Web UI |
| **Egress** | All | All | `0.0.0.0/0` | Outbound (Docker pulls, AWS API calls, etc.) |

Both ingress rules are restricted to **your IP only** -- no public access.

---

### Compute (`ec2.tf`)

A single EC2 instance runs the entire stack via Docker Compose.

**Instance Details:**
- **AMI**: Latest Amazon Linux 2023 (`al2023-ami-*`, x86_64, HVM, gp2)
- **Type**: `t3.xlarge` (4 vCPUs, 16 GiB RAM -- needed for Temporal + ML training)
- **Storage**: 30 GB gp3 EBS volume
- **Key Pair**: Created from your `ssh_public_key_path`
- **IAM Instance Profile**: Grants S3 and SQS permissions to the instance (and its Docker containers)

**Metadata Options:**
```hcl
metadata_options {
  http_endpoint               = "enabled"
  http_tokens                 = "optional"
  http_put_response_hop_limit = 2
}
```
The hop limit is set to `2` so Docker containers running on the instance can reach the EC2 instance metadata service (IMDS) to obtain IAM credentials. With the default hop limit of `1`, containers cannot reach IMDS.

**User Data (Bootstrap Script):**

When the instance first boots, this script runs automatically:

```bash
#!/bin/bash
dnf update -y                       # Update all packages
dnf install -y docker git           # Install Docker and Git
systemctl enable docker             # Start Docker on boot
systemctl start docker              # Start Docker now
usermod -aG docker ec2-user         # Let ec2-user run docker commands

# Install Docker Compose plugin
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

echo "Bootstrap complete" > /tmp/bootstrap-done
```

After bootstrap, the user must SSH in, clone the repo, and run Docker Compose manually (see [Deployment Steps](#deployment-steps) below).

---

### S3 Bucket (`s3.tf`)

```
Bucket name: {project_name}-{random_4_char_hex}
Example:     churn-pipeline-a1b2
```

| Setting | Value | Reason |
|---|---|---|
| `force_destroy` | `true` | Allows `terraform destroy` to delete the bucket even if it contains objects |
| **Event Notification** | S3 → SQS | Triggers on `s3:ObjectCreated:*` events |
| **Prefix filter** | `raw/` | Only files uploaded to the `raw/` prefix trigger notifications |

**Data Flow through the bucket:**
```
raw/              ← CSV uploads land here (triggers pipeline)
staging/          ← Intermediate data (cleaned, engineered)
processed/        ← Train/test splits ready for ML
models/{run_id}/  ← Serialized model (model.pkl)
artifacts/{run_id}/ ← Run metadata and metrics (metadata.json)
```

### SQS Queue (`sqs.tf`)

| Setting | Value |
|---|---|
| Queue name | `{project_name}-s3-notifications` |
| Visibility timeout | 60 seconds |
| Message retention | 86,400 seconds (24 hours) |

The queue has an **access policy** that only allows the S3 bucket to send messages to it (verified via source ARN).

When a file is uploaded to `s3://{bucket}/raw/*`, S3 delivers a notification message to this queue. The trigger service polls the queue, parses the S3 event, and starts the Temporal workflow.

---

### IAM (`iam.tf`)

An IAM role is attached to the EC2 instance via an instance profile, giving the Docker containers AWS access without hardcoded credentials.

**Trust Policy:** EC2 service can assume the role.

**Permissions:**

| Policy | Actions | Resources |
|---|---|---|
| **S3 Access** | `GetObject`, `PutObject`, `ListBucket` | The S3 bucket and all objects within it |
| **SQS Access** | `ReceiveMessage`, `DeleteMessage`, `GetQueueUrl`, `GetQueueAttributes` | The SQS queue |

These are **least-privilege** -- only the specific actions needed by the pipeline are allowed, scoped to only the specific resources created by Terraform.

---

### Outputs (`outputs.tf`)

After `terraform apply`, these values are printed:

| Output | Example | Usage |
|---|---|---|
| `ec2_public_ip` | `54.123.45.67` | Connect to the instance |
| `s3_bucket_name` | `churn-pipeline-a1b2` | Upload CSVs / configure workers |
| `sqs_queue_name` | `churn-pipeline-s3-notifications` | Configure trigger service |
| `ssh_command` | `ssh -i ~/.ssh/id_rsa ec2-user@54.123.45.67` | Quick-copy SSH command |
| `temporal_ui_url` | `http://54.123.45.67:8080` | Open Temporal dashboard |
| `instance_id` | `i-0abc123def456` | Stop/start instance to save costs |

---

## Local vs. AWS: What Changes

There are two separate, standalone Docker Compose files -- one per environment:

- **`docker-compose.yml`** — local development (with LocalStack)
- **`docker-compose.aws.yml`** — AWS deployment (real S3/SQS)

| Concern | Local (`docker-compose.yml`) | AWS (`docker-compose.aws.yml`) |
|---|---|---|
| S3 | LocalStack (`http://localstack:4566`) | Real AWS S3 (no endpoint URL) |
| SQS | LocalStack (`http://localstack:4566`) | Real AWS SQS (no endpoint URL) |
| Credentials | Dummy (`test`/`test`) | IAM instance profile (automatic) |
| Bucket name | `churn-pipeline` | From Terraform output (e.g., `churn-pipeline-a1b2`) |
| Queue name | `s3-notifications` | From Terraform output (`churn-pipeline-s3-notifications`) |
| LocalStack | Running | Not present |

The AWS file has no LocalStack service at all. Without endpoint URLs or hardcoded credentials, boto3 automatically uses real AWS and picks up credentials from the EC2 IAM instance profile.

---

## Deployment Steps

### 1. Provision Infrastructure

```bash
cd terraform/
terraform init
terraform apply -var="allowed_ip=YOUR_IP/32"
```

This creates all 10 resources. Note the outputs.

### 2. SSH into the EC2 Instance

```bash
ssh -i ~/.ssh/id_rsa ec2-user@<ec2_public_ip>
```

Wait for bootstrap to complete (check for `/tmp/bootstrap-done`).

### 3. Clone the Repo and Start Services

```bash
git clone <repo_url>
cd temporal-datapipeline

# Set environment variables from Terraform outputs
export PIPELINE_S3_BUCKET=<s3_bucket_name>
export PIPELINE_SQS_QUEUE_NAME=<sqs_queue_name>

# Start the AWS compose file
docker compose -f docker-compose.aws.yml up -d
```

### 4. Trigger the Pipeline

Upload a CSV to the S3 bucket's `raw/` prefix:

```bash
aws s3 cp data/sample_churn.csv s3://<s3_bucket_name>/raw/sample_churn.csv
```

### 5. Monitor

Open the Temporal UI at `http://<ec2_public_ip>:8080` to watch the workflow execute.

---

## Teardown

```bash
cd terraform/
terraform destroy -var="allowed_ip=YOUR_IP/32"
```

This removes all resources including the S3 bucket (even if non-empty, due to `force_destroy = true`).

---

## Cost Estimate

| Resource | Approximate Monthly Cost |
|---|---|
| EC2 t3.xlarge (24/7) | ~$122 |
| EBS 30 GB gp3 | ~$2.40 |
| S3 (minimal storage) | < $1 |
| SQS (low volume) | < $1 |
| **Total** | **~$126/month** |

To save costs, stop the EC2 instance when not in use:
```bash
aws ec2 stop-instances --instance-ids <instance_id>
aws ec2 start-instances --instance-ids <instance_id>
```
