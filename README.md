# Temporal ML Pipeline — Customer Churn Prediction

An ML pipeline orchestrated by [Temporal](https://temporal.io/) that predicts customer churn. A CSV upload to S3 triggers the pipeline, which runs through ingestion, preprocessing, model training, and evaluation — storing model artifacts back to S3.

The goal of this project is to learn Temporal workflows and AWS deployment, not to build the best ML model.

## Architecture

```
S3 CSV Upload
  -> SQS notification
  -> Trigger Service (polls SQS, starts parent workflow)
  -> Parent Workflow (ChurnPipelineWorkflow)
      -> Child 1: IngestionWorkflow     [default-queue]  — download CSV, validate schema
      -> Child 2: PreprocessingWorkflow [default-queue]  — clean, feature engineer, train/test split
      -> Child 3: TrainingWorkflow      [training-queue] — train RandomForest, serialize model
      -> Child 4: EvaluationWorkflow    [default-queue]  — evaluate model, store artifacts
```

### Key Concepts

- **Parent/Child Workflows**: The parent orchestrates 4 child workflows sequentially, passing results between stages via return values.
- **Task Queues**: Two queues — `default-queue` for most work, `training-queue` for model training (can be scaled independently).
- **Data Flow**: All inter-stage data passes through S3 (not local file paths), so stages can run on different machines.
- **Idempotency**: Workflow ID is derived from the S3 object key (`churn-pipeline-{s3_key}`), so duplicate SQS messages are deduplicated by Temporal.
- **Retry Policies**: Each activity has its own retry policy and timeout. Training and evaluation activities use heartbeats for faster failure detection.

## Project Structure

```
temporal-datapipeline/
├── docker-compose.yml                 # Temporal server, UI, PostgreSQL, LocalStack, workers, trigger
├── Dockerfile.worker                  # Docker image for workers and trigger
├── pyproject.toml                     # Dependencies and project config
├── src/
│   ├── config.py                      # Pydantic settings (S3, SQS, Temporal endpoints)
│   ├── s3_client.py                   # Thin boto3 wrapper for S3 operations
│   ├── trigger.py                     # Standalone SQS poller — starts parent workflow on S3 events
│   ├── models/
│   │   └── churn_model.py             # scikit-learn RandomForestClassifier training/serialization
│   ├── workers/
│   │   ├── default_worker.py          # Worker for default-queue (parent + ingestion + preprocessing + evaluation)
│   │   └── training_worker.py         # Worker for training-queue (model training)
│   └── workflows/
│       ├── parent.py                  # ChurnPipelineWorkflow — orchestrates all child workflows
│       ├── ingestion.py               # Download CSV from S3, validate schema
│       ├── preprocessing.py           # Clean data, feature engineering, train/test split
│       ├── training.py                # Train model, serialize to S3
│       └── evaluation.py              # Evaluate model, store metrics/artifacts to S3
├── docker-compose.aws.yml             # AWS override (replaces LocalStack with real S3/SQS)
├── data/
│   └── sample_churn.csv               # Sample 20-row dataset for testing
├── tests/
│   ├── conftest.py                    # Shared fixtures (moto S3 mocks)
│   ├── test_config.py
│   ├── test_s3_client.py
│   ├── test_ingestion.py
│   ├── test_preprocessing.py
│   ├── test_training.py
│   ├── test_evaluation.py
│   ├── test_parent_workflow.py
│   └── test_trigger.py
├── scripts/
│   ├── localstack-init.sh             # Creates S3 bucket, SQS queue, and event notification in LocalStack
│   └── upload_csv.sh                  # Helper to upload a CSV to LocalStack S3
└── terraform/                         # AWS infrastructure as code
    ├── main.tf                        # Provider config
    ├── variables.tf                   # Input variables (region, instance type, allowed IP)
    ├── outputs.tf                     # EC2 IP, S3 bucket name, SSH command
    ├── vpc.tf                         # VPC, subnet, internet gateway, route table
    ├── security_group.tf              # SSH + Temporal UI access restricted to your IP
    ├── iam.tf                         # EC2 role with S3/SQS permissions
    ├── ec2.tf                         # EC2 instance with Docker bootstrap
    ├── s3.tf                          # S3 bucket with SQS event notification
    ├── sqs.tf                         # SQS queue for S3 events
    └── terraform.tfvars.example       # Example variable values
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [Python 3.11+](https://www.python.org/downloads/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)

## Local Setup

### 1. Install Python dependencies

```bash
uv sync --dev
```

### 2. Run unit tests

```bash
uv run pytest tests/ -v
```

All 21 tests should pass. Tests use [moto](https://github.com/getmoto/moto) to mock AWS services — no Docker required.

### 3. Start the infrastructure

```bash
docker compose up --build -d
```

This starts 7 containers:

| Container | Purpose |
|-----------|---------|
| `postgresql` | Temporal persistence store |
| `temporal-server` | Temporal server (auto-setup with PostgreSQL) |
| `temporal-ui` | Temporal Web UI at http://localhost:8080 |
| `localstack` | Simulates S3 and SQS locally |
| `default-worker` | Processes default-queue (parent, ingestion, preprocessing, evaluation) |
| `training-worker` | Processes training-queue (model training) |
| `trigger` | Polls SQS for S3 upload events, starts pipeline workflows |

Wait until all containers are healthy:

```bash
docker compose ps
```

### 4. Trigger the pipeline

Upload a CSV to the `raw/` prefix in the LocalStack S3 bucket:

```bash
uv run python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')
s3.upload_file('data/sample_churn.csv', 'churn-pipeline', 'raw/sample_churn.csv')
print('Uploaded! Pipeline should start shortly.')
"
```

Or if you have the AWS CLI installed:

```bash
aws --endpoint-url=http://localhost:4566 s3 cp data/sample_churn.csv s3://churn-pipeline/raw/sample_churn.csv
```

### 5. Monitor the pipeline

Open the Temporal Web UI at **http://localhost:8080**.

You should see a `churn-pipeline-raw/sample_churn.csv` workflow with 4 child workflows executing sequentially.

You can also list workflows via the CLI:

```bash
docker exec temporal-datapipeline-temporal-server-1 tctl --address temporal-server:7233 workflow list
```

### 6. Verify artifacts

Check that model artifacts were created in S3:

```bash
uv run python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')
for obj in s3.list_objects_v2(Bucket='churn-pipeline').get('Contents', []):
    print(f'{obj[\"Key\"]:60s} {obj[\"Size\"]:>8d} bytes')
"
```

Expected output:

```
artifacts/.../metadata.json                                      ~410 bytes
models/.../model.pkl                                          ~104000 bytes
processed/test.csv                                               ~335 bytes
processed/train.csv                                              ~745 bytes
raw/sample_churn.csv                                            ~1294 bytes
staging/cleaned_sample_churn.csv                                ~1276 bytes
staging/engineered_sample_churn.csv                              ~886 bytes
staging/sample_churn.csv                                        ~1294 bytes
```

View the model metrics:

```bash
uv run python -c "
import boto3, json
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')
data = s3.get_object(Bucket='churn-pipeline', Key='artifacts/churn-pipeline-raw/sample_churn.csv/metadata.json')['Body'].read()
print(json.dumps(json.loads(data), indent=2))
"
```

### 7. Stop everything

```bash
docker compose down
```

## Configuration

All settings are configured via environment variables with the `PIPELINE_` prefix. See `src/config.py` for defaults.

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_TEMPORAL_HOST` | `localhost:7233` | Temporal server address |
| `PIPELINE_S3_ENDPOINT_URL` | `http://localhost:4566` | S3 endpoint (LocalStack locally, `None` for real AWS) |
| `PIPELINE_SQS_ENDPOINT_URL` | `http://localhost:4566` | SQS endpoint |
| `PIPELINE_S3_BUCKET` | `churn-pipeline` | S3 bucket name |
| `PIPELINE_DEFAULT_TASK_QUEUE` | `default-queue` | Temporal task queue for most workflows |
| `PIPELINE_TRAINING_TASK_QUEUE` | `training-queue` | Temporal task queue for model training |

## AWS Deployment

Deploy the same pipeline to AWS on a single EC2 instance running Docker Compose. Terraform manages all infrastructure.

### Prerequisites

- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) configured with credentials (`aws configure`)
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.0
- An SSH key at `~/.ssh/id_rsa.pub` (generate with `ssh-keygen -t rsa -b 4096` if missing)

### 1. Configure Terraform variables

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set your IP address:

```bash
# Find your IP
curl -s https://checkip.amazonaws.com

# Edit terraform.tfvars
# allowed_ip = "YOUR_IP/32"
```

### 2. Deploy infrastructure

```bash
terraform init
terraform plan    # Review what will be created
terraform apply   # Type 'yes' to confirm
```

This creates: VPC, EC2 instance (t3.xlarge), S3 bucket, SQS queue, IAM roles, and security groups. Takes ~2 minutes.

Note the outputs:

```
ec2_public_ip   = "x.x.x.x"
s3_bucket_name  = "churn-pipeline-xxxxxxxx"
sqs_queue_name  = "churn-pipeline-s3-notifications"
ssh_command     = "ssh -i ~/.ssh/id_rsa ec2-user@x.x.x.x"
temporal_ui_url = "http://x.x.x.x:8080"
```

### 3. Deploy the pipeline to EC2

SSH into the instance:

```bash
$(terraform output -raw ssh_command)
```

Wait for the bootstrap to complete (Docker installation takes ~1-2 minutes after instance launch):

```bash
cat /tmp/bootstrap-done
# Should print: "Bootstrap complete"
```

Copy the project files from your local machine (run this locally, not on EC2):

```bash
cd /path/to/temporal-datapipeline
tar czf /tmp/pipeline.tar.gz \
  --exclude='.venv' --exclude='__pycache__' --exclude='.git' \
  --exclude='terraform/.terraform' --exclude='terraform/*.tfstate*' \
  --exclude='terraform/terraform.tfvars' --exclude='.pytest_cache' .
scp -i ~/.ssh/id_rsa /tmp/pipeline.tar.gz ec2-user@<EC2_IP>:~/pipeline.tar.gz
```

On EC2, extract and start:

```bash
mkdir -p ~/temporal-datapipeline && cd ~/temporal-datapipeline
tar xzf ~/pipeline.tar.gz

# Set environment variables from Terraform outputs
export PIPELINE_S3_BUCKET="<s3_bucket_name from terraform output>"
export PIPELINE_SQS_QUEUE_NAME="<sqs_queue_name from terraform output>"

# Build and start
docker compose -f docker-compose.yml -f docker-compose.aws.yml up --build -d
```

Verify all containers are running:

```bash
docker ps
```

Expected: 7 containers (postgresql, temporal-server, temporal-ui, localstack placeholder, default-worker, training-worker, trigger).

### 4. Run the pipeline

From your local machine, upload a CSV to S3:

```bash
BUCKET=$(cd terraform && terraform output -raw s3_bucket_name)
aws s3 cp data/sample_churn.csv s3://$BUCKET/raw/sample_churn.csv
```

### 5. Monitor

Open the Temporal Web UI in your browser:

```
http://<EC2_PUBLIC_IP>:8080
```

You should see the `churn-pipeline-raw/sample_churn.csv` workflow with 4 child workflows.

### 6. Verify artifacts

```bash
aws s3 ls s3://$BUCKET/ --recursive
```

View model metrics:

```bash
aws s3 cp s3://$BUCKET/artifacts/churn-pipeline-raw/sample_churn.csv/metadata.json - | python3 -m json.tool
```

### 7. Teardown

Destroy all AWS resources when done:

```bash
cd terraform
terraform destroy   # Type 'yes' to confirm
```

To save money without destroying everything, stop the EC2 instance:

```bash
aws ec2 stop-instances --instance-ids $(cd terraform && terraform output -raw instance_id)
```

Restart later with:

```bash
aws ec2 start-instances --instance-ids $(cd terraform && terraform output -raw instance_id)
```

### Cost

| Resource | Monthly Cost (24/7) |
|----------|-------------|
| EC2 t3.xlarge | ~$122 |
| S3 + SQS | < $1 |
| **Total** | **~$122/month** |

Stop or destroy when not in use. A few hours of experimenting costs < $1.

## Tech Stack

- **Python 3.11+** with [temporalio](https://github.com/temporalio/sdk-python) SDK
- **scikit-learn** — RandomForestClassifier for churn prediction
- **pandas** — data processing
- **boto3** — S3 and SQS access
- **pydantic-settings** — configuration management
- **Docker Compose** — local and AWS orchestration
- **LocalStack** — local AWS simulation (S3, SQS)
- **Terraform** — AWS infrastructure as code
- **PostgreSQL** — Temporal persistence store
