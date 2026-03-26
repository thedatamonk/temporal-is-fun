# Temporal ML Pipeline — Customer Churn Prediction

## Overview

An ML pipeline orchestrated by Temporal that predicts customer churn. The pipeline is triggered by a CSV upload to S3, runs through ingestion, preprocessing, training, and evaluation stages, and stores model artifacts back to S3.

The goal is learning Temporal workflows and AWS deployment — not building the best ML model.

## Architecture

### Pipeline Flow

```
S3 CSV Upload
  → SQS notification
  → Trigger Worker (polls SQS, starts parent workflow)
  → Parent Workflow
      → Child 1: Data Ingestion (reads CSV from S3, validates schema)
      → Child 2: Data Preprocessing (cleaning, encoding, train/test split)
      → Child 3: Model Training (trains scikit-learn model, on training-queue)
      → Child 4: Evaluation & Storage (evaluates model, stores artifacts to S3)
```

### Task Queues

- `default-queue` — Runs the trigger worker, parent workflow, ingestion, preprocessing, and evaluation.
- `training-queue` — Dedicated to model training. Separate worker, can be scaled independently with more CPU/memory.

### Trigger Service

The trigger (`trigger.py`) is a standalone process — not a Temporal worker. It polls SQS for S3 event notifications and starts a parent workflow execution via the Temporal client. It runs as its own container in both Docker Compose and ECS.

### Data Flow Between Stages

All data between child workflows passes through S3 — not local file paths. This ensures stages work regardless of which worker/machine they run on. Each child workflow returns serializable metadata (S3 paths, metrics dicts, row counts). The parent passes these as inputs to the next child.

### Workflow ID & Namespace

- **Namespace:** `default`
- **Workflow ID:** `churn-pipeline-{s3_object_key}` — uses the S3 object key to ensure idempotency. If SQS delivers the same message twice, Temporal deduplicates via the workflow ID.

### Idempotency

SQS has at-least-once delivery. Duplicate messages are handled by using the S3 object key as the Temporal workflow ID. Temporal rejects a second `start_workflow` call with the same ID if one is already running.

## Child Workflows & Activities

### Child 1: Data Ingestion Workflow

- **Activity: `download_from_s3`** — Downloads CSV from S3, returns raw data as bytes or uploads a validated copy to a staging S3 prefix.
- **Activity: `validate_schema`** — Checks expected columns exist, correct dtypes, no empty file.
- **Returns:** S3 path to validated CSV in staging prefix + row count metadata.

### Child 2: Data Preprocessing Workflow

- **Activity: `clean_data`** — Handle missing values, drop duplicates, basic outlier removal.
- **Activity: `feature_engineer`** — Encode categoricals (label/one-hot), scale numerics.
- **Activity: `split_data`** — Train/test split (80/20), upload train and test sets to S3.
- **Returns:** S3 paths to train and test datasets.

### Child 3: Model Training Workflow (training-queue)

- **Activity: `train_model`** — Trains a RandomForestClassifier on the training set.
- **Activity: `serialize_model`** — Pickles the model, uploads to S3.
- **Returns:** S3 path to serialized model + training metadata (hyperparams, training time).

### Child 4: Evaluation & Storage Workflow

- **Activity: `evaluate_model`** — Downloads model + test set from S3, computes accuracy/precision/recall/F1/confusion matrix.
- **Activity: `store_artifacts`** — Writes a metadata JSON to S3 with: model path, metrics, dataset info, timestamp, pipeline run ID.
- **Returns:** Final metrics dict + artifact S3 paths.

## Error Handling & Retry Policies

### Per-Activity Retries

| Activity | Retries | Initial Backoff | Multiplier | Start-to-Close Timeout | Heartbeat Timeout | Notes |
|----------|---------|-----------------|------------|----------------------|-------------------|-------|
| `download_from_s3` | 3 | 5s | 2x | 5 min | — | Transient S3 errors |
| `validate_schema` | 0 | — | — | 2 min | — | Fail fast, bad schema won't fix itself |
| `clean_data` | 2 | 2s | 2x | 5 min | — | Protect against OOM/temp file issues |
| `feature_engineer` | 2 | 2s | 2x | 5 min | — | Same as above |
| `split_data` | 2 | 2s | 2x | 5 min | — | Same as above |
| `train_model` | 2 | 5s | 2x | 10 min | 30s | Heaviest stage; heartbeat detects dead workers mid-training |
| `serialize_model` | 3 | 5s | 2x | 5 min | — | S3 write |
| `evaluate_model` | 3 | 5s | 2x | 5 min | 30s | S3 reads + compute |
| `store_artifacts` | 3 | 5s | 2x | 5 min | — | S3 write |

### Workflow-Level Timeouts

- Parent workflow: 60 min execution timeout (4 sequential children, each up to 15 min).
- Each child workflow: 15 min execution timeout.

### Heartbeating

`train_model` and `evaluate_model` send periodic heartbeats (every ~20s). If a worker dies mid-activity, Temporal detects failure within 30s instead of waiting for the full start-to-close timeout.

### Failure Behavior

- If any child workflow fails after retries, the parent workflow fails. Temporal records the full error chain.
- No automatic re-trigger. A failed pipeline requires a new CSV upload or manual re-run via Temporal UI.
- All failures visible in Temporal Web UI.

## Project Structure

```
temporal-datapipeline/
├── docker-compose.yml
├── Dockerfile.worker
├── pyproject.toml
├── src/
│   ├── workflows/
│   │   ├── parent.py          # Parent orchestration workflow
│   │   ├── ingestion.py       # Child workflow + activities
│   │   ├── preprocessing.py   # Child workflow + activities
│   │   ├── training.py        # Child workflow + activities
│   │   └── evaluation.py      # Child workflow + activities
│   ├── workers/
│   │   ├── default_worker.py  # Starts default-queue worker
│   │   └── training_worker.py # Starts training-queue worker
│   ├── trigger.py             # SQS poller → starts parent workflow
│   ├── config.py              # Environment-based config (local vs AWS)
│   └── models/
│       └── churn_model.py     # scikit-learn model definition
├── data/
│   └── sample_churn.csv       # Sample dataset for testing
├── tests/
│   ├── test_workflows.py
│   └── test_activities.py
├── terraform/                 # Phase 2: AWS deployment
│   ├── modules/
│   │   ├── vpc/
│   │   ├── ecs/
│   │   ├── s3/
│   │   ├── sqs/
│   │   └── iam/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars
└── docs/
```

## Local Development Setup

### Docker Compose Services

| Container | Purpose |
|-----------|---------|
| `temporal-server` | Temporal server (official image) |
| `temporal-ui` | Temporal Web UI at `localhost:8080` |
| `postgresql` | Temporal persistence store |
| `default-worker` | Runs parent workflow, ingestion, preprocessing, evaluation |
| `training-worker` | Runs model training on `training-queue` |
| `trigger` | Standalone process: polls SQS, starts parent workflows |
| `localstack` | Simulates S3 + SQS locally |

### Development Workflow

1. `docker-compose up` — starts Temporal + PostgreSQL + LocalStack.
2. Run workers locally (or in containers) connecting to local Temporal.
3. Upload CSV to LocalStack S3 → triggers pipeline.
4. Monitor in Temporal UI at `localhost:8080`.

### Phase 1 Completion Criteria

Upload a CSV to LocalStack S3, the full pipeline runs end-to-end, and model artifacts appear in LocalStack S3.

## AWS Deployment (Phase 2)

See `docs/superpowers/specs/2026-03-25-aws-deployment-design.md` for the full Phase 2 spec.

**Summary:** Single EC2 instance (t3.xlarge) running the same Docker Compose setup. Terraform provisions VPC, EC2, S3, SQS, IAM. No ECS/Fargate — Docker Compose on EC2 is simpler and reuses the local setup directly.

## Technology Stack

- **Language:** Python 3.11+
- **Temporal SDK:** temporalio (Python SDK)
- **ML:** scikit-learn (RandomForestClassifier)
- **Data:** pandas
- **Infrastructure:** Terraform
- **Containers:** Docker, Docker Compose
- **Local AWS simulation:** LocalStack
- **Dataset:** Kaggle-style customer churn CSV (bundled in repo)

## Testing Strategy

- **Activity unit tests:** Plain pytest tests for each activity function. Mock S3 calls with moto or pass LocalStack endpoint.
- **Workflow tests:** Use Temporal's `WorkflowEnvironment` (test server) to run workflows in isolation without Docker.
- **Integration tests:** Require `docker-compose up`. Upload a CSV to LocalStack S3, verify the full pipeline completes and artifacts appear.

## Key Dependencies

- `temporalio` >= 1.7
- `scikit-learn` >= 1.4
- `pandas` >= 2.1
- `boto3` >= 1.34
- `pydantic` >= 2.0 (for data classes / config validation)
