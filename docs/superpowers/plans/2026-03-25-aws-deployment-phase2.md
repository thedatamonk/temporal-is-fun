# Phase 2: AWS Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the existing Temporal ML pipeline to AWS on a single EC2 instance running Docker Compose, provisioned entirely via Terraform.

**Architecture:** Terraform provisions a VPC, EC2 instance (t3.xlarge), S3 bucket, SQS queue, and IAM roles in us-east-1. The EC2 instance runs the same Docker Compose setup as local, with a `docker-compose.aws.yml` override that swaps LocalStack for real AWS services. Workers get AWS credentials via the EC2 instance IAM role.

**Tech Stack:** Terraform, AWS (VPC, EC2, S3, SQS, IAM), Docker Compose, existing Python pipeline code

**Spec:** `docs/superpowers/specs/2026-03-25-aws-deployment-design.md`

---

## File Structure

```
temporal-datapipeline/
├── src/
│   └── config.py                      # MODIFY: Change credential defaults to None
├── docker-compose.yml                 # EXISTING: Base file, unchanged
├── docker-compose.aws.yml             # CREATE: AWS override (no LocalStack, real S3/SQS)
├── .gitignore                         # CREATE: Ignore terraform state, tfvars
└── terraform/
    ├── main.tf                        # Provider config, data sources
    ├── variables.tf                   # Input variables
    ├── outputs.tf                     # EC2 IP, S3 bucket, SSH command
    ├── vpc.tf                         # VPC, subnet, IGW, route table
    ├── security_group.tf              # Security group rules
    ├── iam.tf                         # IAM role, policy, instance profile
    ├── ec2.tf                         # EC2 instance, key pair, user data
    ├── s3.tf                          # S3 bucket, event notification
    ├── sqs.tf                         # SQS queue, queue policy
    └── terraform.tfvars.example       # Example values (actual tfvars is gitignored)
```

---

## Task 1: Prerequisites — AWS CLI & Terraform

**Files:** None (setup steps only)

- [ ] **Step 1: Verify AWS CLI is installed**

```bash
aws --version
```

If not installed:
```bash
# macOS
brew install awscli
```

- [ ] **Step 2: Configure AWS credentials**

```bash
aws configure
```

Enter your AWS Access Key ID, Secret Access Key, region (`us-east-1`), and output format (`json`).

- [ ] **Step 3: Verify credentials work**

```bash
aws sts get-caller-identity
```

Expected: JSON with your Account, UserId, and Arn.

- [ ] **Step 4: Verify Terraform is installed**

```bash
terraform --version
```

If not installed:
```bash
# macOS
brew install terraform
```

- [ ] **Step 5: Verify SSH key exists**

```bash
ls ~/.ssh/id_rsa.pub
```

If not found, generate one:
```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
```

---

## Task 2: Update Config Defaults for AWS Compatibility

**Files:**
- Modify: `src/config.py`
- Modify: `docker-compose.yml`
- Modify: `tests/conftest.py`

- [ ] **Step 1: Change credential defaults to None in config.py**

In `src/config.py`, change lines 15-16:

```python
# Before:
aws_access_key_id: str = "test"
aws_secret_access_key: str = "test"

# After:
aws_access_key_id: str | None = None
aws_secret_access_key: str | None = None
```

This lets boto3 fall back to the EC2 instance role on AWS. Locally, docker-compose.yml will explicitly set them to `"test"`.

- [ ] **Step 2: Add credential env vars to docker-compose.yml**

Add `PIPELINE_AWS_ACCESS_KEY_ID=test` and `PIPELINE_AWS_SECRET_ACCESS_KEY=test` to each worker and trigger service in `docker-compose.yml`. This ensures local development still works with LocalStack after the config default change.

For `default-worker`, `training-worker`, and `trigger`, add to each `environment` block:

```yaml
      - PIPELINE_AWS_ACCESS_KEY_ID=test
      - PIPELINE_AWS_SECRET_ACCESS_KEY=test
```

- [ ] **Step 3: Update test fixture in conftest.py**

In `tests/conftest.py`, the `aws_settings` fixture already sets `aws_access_key_id="testing"` and `aws_secret_access_key="testing"`, so tests are unaffected. Verify:

```bash
uv run pytest tests/ -v
```

Expected: 21 passed.

---

## Task 3: Create .gitignore

**Files:**
- Create: `.gitignore`

- [ ] **Step 1: Create .gitignore**

```gitignore
# Python
__pycache__/
*.pyc
.venv/
*.egg-info/
.pytest_cache/

# Terraform
terraform/.terraform/
terraform/*.tfstate
terraform/*.tfstate.backup
terraform/terraform.tfvars
terraform/.terraform.lock.hcl

# IDE
.idea/
.vscode/

# OS
.DS_Store
```

---

## Task 4: Terraform — Provider & Variables

**Files:**
- Create: `terraform/main.tf`
- Create: `terraform/variables.tf`
- Create: `terraform/terraform.tfvars.example`

- [ ] **Step 1: Create terraform/main.tf**

```hcl
terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}
```

- [ ] **Step 2: Create terraform/variables.tf**

```hcl
variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.xlarge"
}

variable "ssh_public_key_path" {
  description = "Path to SSH public key for EC2 access"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "allowed_ip" {
  description = "Your IP address for SSH and UI access (CIDR format, e.g., 1.2.3.4/32)"
  type        = string
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "churn-pipeline"
}
```

- [ ] **Step 3: Create terraform/terraform.tfvars.example**

```hcl
# Copy this to terraform.tfvars and fill in your values
# Find your IP: curl -s https://checkip.amazonaws.com

allowed_ip          = "YOUR_IP/32"
ssh_public_key_path = "~/.ssh/id_rsa.pub"
```

- [ ] **Step 4: Verify terraform init works**

```bash
cd terraform && terraform init
```

Expected: "Terraform has been successfully initialized!"

---

## Task 5: Terraform — VPC & Networking

**Files:**
- Create: `terraform/vpc.tf`

- [ ] **Step 1: Create terraform/vpc.tf**

```hcl
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-subnet"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}
```

- [ ] **Step 2: Validate**

```bash
terraform validate
```

Expected: "Success! The configuration is valid."

---

## Task 6: Terraform — Security Group

**Files:**
- Create: `terraform/security_group.tf`

- [ ] **Step 1: Create terraform/security_group.tf**

```hcl
resource "aws_security_group" "ec2" {
  name        = "${var.project_name}-ec2-sg"
  description = "Security group for Temporal pipeline EC2 instance"
  vpc_id      = aws_vpc.main.id

  # SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ip]
    description = "SSH from user IP"
  }

  # Temporal UI
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ip]
    description = "Temporal UI from user IP"
  }

  # All outbound
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }

  tags = {
    Name = "${var.project_name}-ec2-sg"
  }
}
```

- [ ] **Step 2: Validate**

```bash
terraform validate
```

---

## Task 7: Terraform — IAM Role & Instance Profile

**Files:**
- Create: `terraform/iam.tf`

- [ ] **Step 1: Create terraform/iam.tf**

```hcl
resource "aws_iam_role" "ec2" {
  name = "${var.project_name}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "${var.project_name}-ec2-role"
  }
}

resource "aws_iam_role_policy" "s3_access" {
  name = "${var.project_name}-s3-access"
  role = aws_iam_role.ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.pipeline.arn,
          "${aws_s3_bucket.pipeline.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "sqs_access" {
  name = "${var.project_name}-sqs-access"
  role = aws_iam_role.ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueUrl",
          "sqs:GetQueueAttributes"
        ]
        Resource = aws_sqs_queue.s3_notifications.arn
      }
    ]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.project_name}-ec2-profile"
  role = aws_iam_role.ec2.name
}
```

- [ ] **Step 2: Validate**

```bash
terraform validate
```

---

## Task 8: Terraform — S3 Bucket & SQS Queue

**Files:**
- Create: `terraform/s3.tf`
- Create: `terraform/sqs.tf`

- [ ] **Step 1: Create terraform/sqs.tf**

```hcl
resource "aws_sqs_queue" "s3_notifications" {
  name                       = "${var.project_name}-s3-notifications"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400

  tags = {
    Name = "${var.project_name}-s3-notifications"
  }
}

resource "aws_sqs_queue_policy" "allow_s3" {
  queue_url = aws_sqs_queue.s3_notifications.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.s3_notifications.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_s3_bucket.pipeline.arn
          }
        }
      }
    ]
  })
}
```

- [ ] **Step 2: Create terraform/s3.tf**

```hcl
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "pipeline" {
  bucket        = "${var.project_name}-${random_id.bucket_suffix.hex}"
  force_destroy = true

  tags = {
    Name = "${var.project_name}-bucket"
  }
}

resource "aws_s3_bucket_notification" "sqs" {
  bucket = aws_s3_bucket.pipeline.id

  queue {
    queue_arn     = aws_sqs_queue.s3_notifications.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
  }

  depends_on = [aws_sqs_queue_policy.allow_s3]
}
```

- [ ] **Step 3: Add random provider to main.tf**

Add to the `required_providers` block in `terraform/main.tf`:

```hcl
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
```

- [ ] **Step 4: Re-init and validate**

```bash
terraform init && terraform validate
```

---

## Task 9: Terraform — EC2 Instance

**Files:**
- Create: `terraform/ec2.tf`

- [ ] **Step 1: Create terraform/ec2.tf**

```hcl
data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

resource "aws_key_pair" "deployer" {
  key_name   = "${var.project_name}-key"
  public_key = file(var.ssh_public_key_path)
}

resource "aws_instance" "pipeline" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.deployer.key_name
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.ec2.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  # Allow Docker containers to reach EC2 instance metadata for IAM credentials
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "optional"
    http_put_response_hop_limit = 2
  }

  user_data = <<-EOF
    #!/bin/bash
    set -ex

    # Install Docker
    dnf update -y
    dnf install -y docker git
    systemctl enable docker
    systemctl start docker

    # Install Docker Compose plugin
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

    # Add ec2-user to docker group
    usermod -aG docker ec2-user

    echo "Bootstrap complete" > /tmp/bootstrap-done
  EOF

  tags = {
    Name = "${var.project_name}-ec2"
  }
}
```

- [ ] **Step 2: Validate**

```bash
terraform validate
```

---

## Task 10: Terraform — Outputs

**Files:**
- Create: `terraform/outputs.tf`

- [ ] **Step 1: Create terraform/outputs.tf**

```hcl
output "ec2_public_ip" {
  description = "Public IP of the EC2 instance"
  value       = aws_instance.pipeline.public_ip
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.pipeline.id
}

output "sqs_queue_name" {
  description = "Name of the SQS queue"
  value       = aws_sqs_queue.s3_notifications.name
}

output "ssh_command" {
  description = "SSH command to connect to the EC2 instance"
  value       = "ssh -i ${replace(var.ssh_public_key_path, ".pub", "")} ec2-user@${aws_instance.pipeline.public_ip}"
}

output "temporal_ui_url" {
  description = "URL for the Temporal Web UI"
  value       = "http://${aws_instance.pipeline.public_ip}:8080"
}

output "instance_id" {
  description = "EC2 instance ID (for stop/start commands)"
  value       = aws_instance.pipeline.id
}
```

- [ ] **Step 2: Full validate**

```bash
terraform validate
```

Expected: "Success!"

---

## Task 11: Docker Compose AWS Override

**Files:**
- Create: `docker-compose.aws.yml`

- [ ] **Step 1: Create docker-compose.aws.yml**

```yaml
# AWS override — use with: docker compose -f docker-compose.yml -f docker-compose.aws.yml up -d
# Requires PIPELINE_S3_BUCKET env var set before running.

services:
  # Remove LocalStack (using real AWS)
  localstack:
    profiles:
      - disabled

  default-worker:
    depends_on:
      temporal-server:
        condition: service_healthy
    environment:
      - PIPELINE_TEMPORAL_HOST=temporal-server:7233
      - PIPELINE_S3_ENDPOINT_URL=
      - PIPELINE_SQS_ENDPOINT_URL=
      - PIPELINE_S3_BUCKET=${PIPELINE_S3_BUCKET}
      - PIPELINE_S3_REGION=us-east-1
      - PIPELINE_AWS_ACCESS_KEY_ID=
      - PIPELINE_AWS_SECRET_ACCESS_KEY=
      - PIPELINE_SQS_QUEUE_NAME=${PIPELINE_SQS_QUEUE_NAME:-churn-pipeline-s3-notifications}

  training-worker:
    depends_on:
      temporal-server:
        condition: service_healthy
    environment:
      - PIPELINE_TEMPORAL_HOST=temporal-server:7233
      - PIPELINE_S3_ENDPOINT_URL=
      - PIPELINE_SQS_ENDPOINT_URL=
      - PIPELINE_S3_BUCKET=${PIPELINE_S3_BUCKET}
      - PIPELINE_S3_REGION=us-east-1
      - PIPELINE_AWS_ACCESS_KEY_ID=
      - PIPELINE_AWS_SECRET_ACCESS_KEY=

  trigger:
    depends_on:
      temporal-server:
        condition: service_healthy
    environment:
      - PIPELINE_TEMPORAL_HOST=temporal-server:7233
      - PIPELINE_S3_ENDPOINT_URL=
      - PIPELINE_SQS_ENDPOINT_URL=
      - PIPELINE_S3_BUCKET=${PIPELINE_S3_BUCKET}
      - PIPELINE_S3_REGION=us-east-1
      - PIPELINE_AWS_ACCESS_KEY_ID=
      - PIPELINE_AWS_SECRET_ACCESS_KEY=
      - PIPELINE_SQS_QUEUE_NAME=${PIPELINE_SQS_QUEUE_NAME:-churn-pipeline-s3-notifications}
```

Note: Setting `PIPELINE_S3_ENDPOINT_URL=` (empty) makes pydantic parse it as `None`, which tells boto3 to use real AWS. The `localstack` service is disabled via Docker Compose profiles. The `depends_on` override removes the LocalStack dependency.

---

## Task 12: Terraform Plan & Apply

**Files:** None (execution steps)

- [ ] **Step 1: Create terraform.tfvars**

```bash
cd terraform

# Get your public IP
MY_IP=$(curl -s https://checkip.amazonaws.com)

cat > terraform.tfvars <<EOF
allowed_ip          = "${MY_IP}/32"
ssh_public_key_path = "~/.ssh/id_rsa.pub"
EOF

cat terraform.tfvars
```

- [ ] **Step 2: Run terraform plan**

```bash
terraform plan
```

Review the output. Expected: ~12 resources to create (VPC, subnet, IGW, route table, route table association, security group, IAM role, 2 IAM policies, instance profile, key pair, S3 bucket, random_id, S3 notification, SQS queue, SQS policy, EC2 instance).

- [ ] **Step 3: Run terraform apply**

```bash
terraform apply
```

Type `yes` when prompted. Wait 2-3 minutes for all resources to be created.

- [ ] **Step 4: Note the outputs**

```bash
terraform output
```

Save the EC2 public IP, S3 bucket name, and SSH command.

---

## Task 13: Deploy Pipeline to EC2

**Files:** None (deployment steps on EC2)

- [ ] **Step 1: Wait for EC2 bootstrap to complete**

```bash
# SSH into the instance (use the ssh_command output from terraform)
ssh -i ~/.ssh/id_rsa ec2-user@<EC2_PUBLIC_IP>

# Check bootstrap is done (may take 1-2 minutes after instance launches)
cat /tmp/bootstrap-done
```

Expected: "Bootstrap complete"

If the file doesn't exist yet, wait a minute and try again.

- [ ] **Step 2: Verify Docker is running**

```bash
docker --version
docker compose version
```

- [ ] **Step 3: Clone the repo or copy files**

Option A — if repo is on GitHub:
```bash
git clone <your-repo-url>
cd temporal-datapipeline
```

Option B — from your local machine (run locally, not on EC2):
```bash
# From your local machine
rsync -avz --exclude '.venv' --exclude '__pycache__' --exclude '.git' --exclude 'terraform/.terraform' \
  -e "ssh -i ~/.ssh/id_rsa" \
  /Users/rohil/rohil-workspace/temporal-datapipeline/ \
  ec2-user@<EC2_PUBLIC_IP>:~/temporal-datapipeline/
```

- [ ] **Step 4: Export environment variables and start**

On EC2:
```bash
cd ~/temporal-datapipeline

# Set the bucket name and SQS queue name from Terraform outputs
export PIPELINE_S3_BUCKET="<bucket-name-from-terraform-output>"
export PIPELINE_SQS_QUEUE_NAME="<sqs-queue-name-from-terraform-output>"

# Build and start
docker compose -f docker-compose.yml -f docker-compose.aws.yml up --build -d
```

- [ ] **Step 5: Verify all containers are running**

```bash
docker compose -f docker-compose.yml -f docker-compose.aws.yml ps
```

Expected: 6 containers running (postgresql, temporal-server, temporal-ui, default-worker, training-worker, trigger). LocalStack should NOT appear.

- [ ] **Step 6: Check worker logs**

```bash
docker compose -f docker-compose.yml -f docker-compose.aws.yml logs default-worker
docker compose -f docker-compose.yml -f docker-compose.aws.yml logs trigger
```

No errors expected.

---

## Task 14: End-to-End Test on AWS

**Files:** None (verification steps)

- [ ] **Step 1: Upload CSV to real S3 (from your local machine)**

```bash
BUCKET=$(cd terraform && terraform output -raw s3_bucket_name)
aws s3 cp data/sample_churn.csv s3://$BUCKET/raw/sample_churn.csv
```

- [ ] **Step 2: Open Temporal UI**

Open in your browser:
```
http://<EC2_PUBLIC_IP>:8080
```

You should see the `churn-pipeline-raw/sample_churn.csv` workflow with 4 child workflows.

- [ ] **Step 3: Verify artifacts in S3**

```bash
aws s3 ls s3://$BUCKET/ --recursive
```

Expected: Same artifact structure as local (raw/, staging/, processed/, models/, artifacts/).

- [ ] **Step 4: Check model metrics**

```bash
aws s3 cp s3://$BUCKET/artifacts/churn-pipeline-raw/sample_churn.csv/metadata.json - | python3 -m json.tool
```

Expected: JSON with accuracy, precision, recall, f1 metrics.

---

## Task 15: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add AWS deployment section to README**

Add a new section after the "Local Setup" section covering:
- Prerequisites (AWS CLI, Terraform, SSH key)
- `aws configure`
- Terraform init/apply
- SSH + docker compose
- Upload CSV, monitor Temporal UI
- Teardown (`terraform destroy`)
- Cost warning

---

## Task Summary

| Task | Description | Dependencies |
|------|-------------|-------------|
| 1 | Prerequisites (AWS CLI, Terraform, SSH key) | None |
| 2 | Update config defaults for AWS compatibility | None |
| 3 | Create .gitignore | None |
| 4 | Terraform — provider & variables | Task 1 |
| 5 | Terraform — VPC & networking | Task 4 |
| 6 | Terraform — security group | Task 5 |
| 7 | Terraform — IAM role | Task 4 |
| 8 | Terraform — S3 & SQS | Task 4 |
| 9 | Terraform — EC2 instance | Tasks 5, 6, 7, 8 |
| 10 | Terraform — outputs | Task 9 |
| 11 | Docker Compose AWS override | Task 2 |
| 12 | Terraform plan & apply | Task 10 |
| 13 | Deploy pipeline to EC2 | Tasks 11, 12 |
| 14 | E2E test on AWS | Task 13 |
| 15 | Update README | Task 14 |
