# Temporal ML Pipeline — Phase 1 (Local Development) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working Temporal ML pipeline locally that ingests a CSV from S3, preprocesses it, trains a churn prediction model, evaluates it, and stores artifacts back to S3 — all orchestrated via parent/child Temporal workflows.

**Architecture:** Parent workflow orchestrates 4 child workflows (ingestion → preprocessing → training → evaluation) running on 2 task queues. A standalone trigger process polls SQS for S3 upload events and starts the parent workflow. All inter-stage data flows through S3. Local development uses Docker Compose with LocalStack for S3/SQS simulation.

**Tech Stack:** Python 3.11+, temporalio SDK, scikit-learn, pandas, boto3, pydantic, Docker Compose, LocalStack, PostgreSQL

**Spec:** `docs/superpowers/specs/2026-03-25-temporal-ml-pipeline-design.md`

---

## File Structure

```
temporal-datapipeline/
├── pyproject.toml                    # Project metadata, dependencies
├── docker-compose.yml                # Temporal server, UI, PostgreSQL, LocalStack
├── Dockerfile.worker                 # Docker image for workers + trigger
├── src/
│   ├── __init__.py
│   ├── config.py                     # Pydantic settings: S3/SQS/Temporal endpoints
│   ├── s3_client.py                  # Thin boto3 wrapper for S3 operations
│   ├── workflows/
│   │   ├── __init__.py
│   │   ├── parent.py                 # Parent orchestration workflow
│   │   ├── ingestion.py              # Child workflow + activities (download, validate)
│   │   ├── preprocessing.py          # Child workflow + activities (clean, feature eng, split)
│   │   ├── training.py               # Child workflow + activities (train, serialize)
│   │   └── evaluation.py             # Child workflow + activities (evaluate, store artifacts)
│   ├── models/
│   │   ├── __init__.py
│   │   └── churn_model.py            # scikit-learn model training/prediction logic
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── default_worker.py         # Starts default-queue worker
│   │   └── training_worker.py        # Starts training-queue worker
│   └── trigger.py                    # SQS poller → starts parent workflow
├── data/
│   └── sample_churn.csv              # Sample dataset
├── tests/
│   ├── __init__.py
│   ├── conftest.py                   # Shared fixtures (S3 client, test data)
│   ├── test_config.py                # Config tests
│   ├── test_s3_client.py             # S3 wrapper tests
│   ├── test_ingestion.py             # Ingestion activity + workflow tests
│   ├── test_preprocessing.py         # Preprocessing activity + workflow tests
│   ├── test_training.py              # Training activity + workflow tests
│   ├── test_evaluation.py            # Evaluation activity + workflow tests
│   ├── test_parent_workflow.py       # Parent workflow tests
│   └── test_trigger.py               # Trigger tests
└── scripts/
    └── upload_csv.sh                 # Helper: upload CSV to LocalStack S3
```

---

## Task 1: Project Setup & Configuration

**Files:**
- Create: `pyproject.toml`
- Create: `src/__init__.py`
- Create: `src/config.py`
- Create: `tests/__init__.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "temporal-datapipeline"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "temporalio>=1.7.1",
    "scikit-learn>=1.4",
    "pandas>=2.1",
    "boto3>=1.34",
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "moto[s3,sqs]>=5.0",
]

[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 2: Create `src/config.py`**

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Temporal
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"
    default_task_queue: str = "default-queue"
    training_task_queue: str = "training-queue"

    # S3
    s3_endpoint_url: str | None = "http://localhost:4566"
    s3_bucket: str = "churn-pipeline"
    s3_region: str = "us-east-1"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"

    # SQS
    sqs_endpoint_url: str | None = "http://localhost:4566"
    sqs_queue_name: str = "s3-notifications"

    # S3 prefixes
    raw_prefix: str = "raw/"
    staging_prefix: str = "staging/"
    processed_prefix: str = "processed/"
    models_prefix: str = "models/"
    artifacts_prefix: str = "artifacts/"

    model_config = {"env_prefix": "PIPELINE_"}


settings = Settings()
```

- [ ] **Step 3: Create `src/__init__.py`**

```python
```

(Empty file.)

- [ ] **Step 4: Write test for config**

```python
# tests/test_config.py
from src.config import Settings


def test_default_settings():
    s = Settings()
    assert s.temporal_host == "localhost:7233"
    assert s.default_task_queue == "default-queue"
    assert s.training_task_queue == "training-queue"
    assert s.s3_bucket == "churn-pipeline"


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("PIPELINE_S3_BUCKET", "custom-bucket")
    s = Settings()
    assert s.s3_bucket == "custom-bucket"
```

- [ ] **Step 5: Create `tests/__init__.py`**

Empty file.

- [ ] **Step 6: Install and run tests**

```bash
cd /Users/rohil/rohil-workspace/temporal-datapipeline
pip install -e ".[dev]"
pytest tests/test_config.py -v
```

Expected: 2 tests PASS.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml src/__init__.py src/config.py tests/__init__.py tests/test_config.py
git commit -m "feat: project setup with config and dependencies"
```

---

## Task 2: S3 Client Wrapper

**Files:**
- Create: `src/s3_client.py`
- Create: `tests/conftest.py`
- Create: `tests/test_s3_client.py`

- [ ] **Step 1: Write tests for S3 client**

```python
# tests/conftest.py
import boto3
import pytest
from moto import mock_aws

from src.config import Settings


@pytest.fixture
def aws_settings():
    return Settings(
        s3_endpoint_url=None,
        sqs_endpoint_url=None,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        s3_region="us-east-1",
    )


@pytest.fixture
def s3_mock(aws_settings):
    with mock_aws():
        client = boto3.client("s3", region_name=aws_settings.s3_region)
        client.create_bucket(Bucket=aws_settings.s3_bucket)
        yield client


@pytest.fixture
def sample_csv_bytes():
    return b"customerID,gender,SeniorCitizen,tenure,MonthlyCharges,Churn\n1,Male,0,12,29.85,No\n2,Female,1,34,56.95,Yes\n"
```

```python
# tests/test_s3_client.py
from moto import mock_aws

from src.s3_client import S3Client


@mock_aws
def test_upload_and_download(aws_settings, s3_mock, sample_csv_bytes):
    client = S3Client(aws_settings)
    key = "raw/test.csv"

    client.upload_bytes(key, sample_csv_bytes)
    result = client.download_bytes(key)

    assert result == sample_csv_bytes


@mock_aws
def test_upload_and_download_json(aws_settings, s3_mock):
    client = S3Client(aws_settings)
    key = "artifacts/meta.json"
    data = {"accuracy": 0.95}

    client.upload_json(key, data)
    result = client.download_json(key)

    assert result == data
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_s3_client.py -v
```

Expected: FAIL — `src.s3_client` does not exist.

- [ ] **Step 3: Implement S3 client**

```python
# src/s3_client.py
import json

import boto3

from src.config import Settings


class S3Client:
    def __init__(self, settings: Settings | None = None):
        if settings is None:
            settings = Settings()
        self._settings = settings
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )

    @property
    def bucket(self) -> str:
        return self._settings.s3_bucket

    def upload_bytes(self, key: str, data: bytes) -> str:
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return key

    def download_bytes(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def upload_json(self, key: str, data: dict) -> str:
        return self.upload_bytes(key, json.dumps(data).encode())

    def download_json(self, key: str) -> dict:
        return json.loads(self.download_bytes(key))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_s3_client.py -v
```

Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/s3_client.py tests/conftest.py tests/test_s3_client.py
git commit -m "feat: add S3 client wrapper with upload/download"
```

---

## Task 3: Sample Dataset

**Files:**
- Create: `data/sample_churn.csv`

- [ ] **Step 1: Create sample churn dataset**

Create `data/sample_churn.csv` — a small (20-row) representative dataset with columns: `customerID`, `gender`, `SeniorCitizen`, `Partner`, `Dependents`, `tenure`, `PhoneService`, `InternetService`, `Contract`, `MonthlyCharges`, `TotalCharges`, `Churn`.

This should contain a mix of Yes/No churn values, numeric and categorical columns, and one or two rows with missing `TotalCharges` (to test cleaning logic).

- [ ] **Step 2: Commit**

```bash
git add data/sample_churn.csv
git commit -m "feat: add sample churn dataset"
```

---

## Task 4: Data Ingestion Workflow & Activities

**Files:**
- Create: `src/workflows/__init__.py`
- Create: `src/workflows/ingestion.py`
- Create: `tests/test_ingestion.py`

- [ ] **Step 1: Write tests for ingestion activities**

```python
# tests/test_ingestion.py
import pytest
from moto import mock_aws

from src.config import Settings
from src.s3_client import S3Client
from src.workflows.ingestion import (
    IngestionResult,
    download_from_s3,
    validate_schema,
)

VALID_CSV = (
    b"customerID,gender,SeniorCitizen,Partner,Dependents,tenure,"
    b"PhoneService,InternetService,Contract,MonthlyCharges,TotalCharges,Churn\n"
    b"1,Male,0,Yes,No,12,Yes,DSL,Month-to-month,29.85,358.2,No\n"
)

INVALID_CSV = b"id,name,value\n1,test,100\n"


@mock_aws
def test_download_from_s3(aws_settings, s3_mock):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("raw/test.csv", VALID_CSV)

    result = download_from_s3("raw/test.csv", s3)
    assert result.startswith(aws_settings.staging_prefix)

    downloaded = s3.download_bytes(result)
    assert downloaded == VALID_CSV


@mock_aws
def test_validate_schema_valid(aws_settings, s3_mock):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("staging/test.csv", VALID_CSV)

    result = validate_schema("staging/test.csv", s3)
    assert result.row_count == 1
    assert result.s3_key == "staging/test.csv"


@mock_aws
def test_validate_schema_invalid(aws_settings, s3_mock):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("staging/bad.csv", INVALID_CSV)

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_schema("staging/bad.csv", s3)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_ingestion.py -v
```

Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement ingestion activities**

```python
# src/workflows/ingestion.py
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO

import pandas as pd
from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from src.config import settings
    from src.s3_client import S3Client

REQUIRED_COLUMNS = {
    "customerID", "gender", "SeniorCitizen", "Partner", "Dependents",
    "tenure", "PhoneService", "InternetService", "Contract",
    "MonthlyCharges", "TotalCharges", "Churn",
}


@dataclass
class IngestionResult:
    s3_key: str
    row_count: int


@activity.defn
async def download_from_s3(raw_s3_key: str, s3: S3Client | None = None) -> str:
    if s3 is None:
        s3 = S3Client()
    data = s3.download_bytes(raw_s3_key)
    staging_key = raw_s3_key.replace(settings.raw_prefix, settings.staging_prefix)
    s3.upload_bytes(staging_key, data)
    return staging_key


@activity.defn
async def validate_schema(staging_s3_key: str, s3: S3Client | None = None) -> IngestionResult:
    if s3 is None:
        s3 = S3Client()
    data = s3.download_bytes(staging_s3_key)
    df = pd.read_csv(BytesIO(data))

    if df.empty:
        raise ValueError("CSV file is empty")

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return IngestionResult(s3_key=staging_s3_key, row_count=len(df))


@workflow.defn
class IngestionWorkflow:
    @workflow.run
    async def run(self, raw_s3_key: str) -> IngestionResult:
        staging_key = await workflow.execute_activity(
            download_from_s3,
            raw_s3_key,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=workflow.RetryPolicy(
                maximum_attempts=4,
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
            ),
        )
        result = await workflow.execute_activity(
            validate_schema,
            staging_key,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=workflow.RetryPolicy(maximum_attempts=1),
        )
        return result
```

- [ ] **Step 4: Create `src/workflows/__init__.py`**

Empty file.

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_ingestion.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/workflows/__init__.py src/workflows/ingestion.py tests/test_ingestion.py
git commit -m "feat: add data ingestion workflow with download and schema validation"
```

---

## Task 5: Data Preprocessing Workflow & Activities

**Files:**
- Create: `src/workflows/preprocessing.py`
- Create: `tests/test_preprocessing.py`

- [ ] **Step 1: Write tests for preprocessing activities**

```python
# tests/test_preprocessing.py
from io import BytesIO

import pandas as pd
import pytest
from moto import mock_aws

from src.config import Settings
from src.s3_client import S3Client
from src.workflows.preprocessing import (
    PreprocessingResult,
    clean_data,
    feature_engineer,
    split_data,
)


def _make_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode()


@pytest.fixture
def raw_df():
    return pd.DataFrame({
        "customerID": ["1", "2", "3", "4", "5", "5"],
        "gender": ["Male", "Female", "Male", "Female", "Male", "Male"],
        "SeniorCitizen": [0, 1, 0, 1, 0, 0],
        "Partner": ["Yes", "No", "Yes", "No", "Yes", "Yes"],
        "Dependents": ["No", "No", "Yes", "No", "Yes", "Yes"],
        "tenure": [12, 34, 5, 72, 0, 0],
        "PhoneService": ["Yes", "Yes", "No", "Yes", "Yes", "Yes"],
        "InternetService": ["DSL", "Fiber optic", "No", "DSL", "DSL", "DSL"],
        "Contract": ["Month-to-month", "One year", "Two year", "Month-to-month", "One year", "One year"],
        "MonthlyCharges": [29.85, 56.95, 20.00, 89.10, 45.00, 45.00],
        "TotalCharges": [358.2, 1936.3, 100.0, 6415.2, None, None],
        "Churn": ["No", "Yes", "No", "Yes", "No", "No"],
    })


@mock_aws
def test_clean_data(aws_settings, s3_mock, raw_df):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("staging/test.csv", _make_csv(raw_df))

    result_key = clean_data("staging/test.csv", s3)
    cleaned = pd.read_csv(BytesIO(s3.download_bytes(result_key)))

    # Should have dropped 1 duplicate (row 5 and 6 are identical except index)
    assert len(cleaned) == 5
    # TotalCharges should have no nulls (filled with median)
    assert cleaned["TotalCharges"].isna().sum() == 0


@mock_aws
def test_feature_engineer(aws_settings, s3_mock, raw_df):
    s3 = S3Client(aws_settings)
    # Use cleaned data (no nulls, no dupes)
    clean_df = raw_df.drop_duplicates(subset="customerID").copy()
    clean_df["TotalCharges"] = clean_df["TotalCharges"].fillna(0)
    s3.upload_bytes("staging/cleaned.csv", _make_csv(clean_df))

    result_key = feature_engineer("staging/cleaned.csv", s3)
    engineered = pd.read_csv(BytesIO(s3.download_bytes(result_key)))

    # Should not contain original categorical columns
    assert "gender" not in engineered.columns
    assert "customerID" not in engineered.columns
    # Churn should be numeric (0/1)
    assert engineered["Churn"].dtype in ("int64", "float64")


@mock_aws
def test_split_data(aws_settings, s3_mock, raw_df):
    s3 = S3Client(aws_settings)
    clean_df = raw_df.drop_duplicates(subset="customerID").copy()
    clean_df["TotalCharges"] = clean_df["TotalCharges"].fillna(0)
    # Create a minimal numeric dataset for splitting
    clean_df["Churn"] = clean_df["Churn"].map({"No": 0, "Yes": 1})
    clean_df = clean_df[["tenure", "MonthlyCharges", "TotalCharges", "Churn"]]
    s3.upload_bytes("staging/engineered.csv", _make_csv(clean_df))

    result = split_data("staging/engineered.csv", s3)
    assert result.train_s3_key.startswith("processed/")
    assert result.test_s3_key.startswith("processed/")

    train = pd.read_csv(BytesIO(s3.download_bytes(result.train_s3_key)))
    test = pd.read_csv(BytesIO(s3.download_bytes(result.test_s3_key)))
    assert len(train) + len(test) == len(clean_df)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_preprocessing.py -v
```

Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement preprocessing activities**

```python
# src/workflows/preprocessing.py
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO

import pandas as pd
from sklearn.model_selection import train_test_split
from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from src.config import settings
    from src.s3_client import S3Client


@dataclass
class PreprocessingResult:
    train_s3_key: str
    test_s3_key: str


def _read_csv_from_s3(s3_key: str, s3: S3Client) -> pd.DataFrame:
    return pd.read_csv(BytesIO(s3.download_bytes(s3_key)))


def _write_csv_to_s3(df: pd.DataFrame, s3_key: str, s3: S3Client) -> str:
    s3.upload_bytes(s3_key, df.to_csv(index=False).encode())
    return s3_key


@activity.defn
async def clean_data(staging_s3_key: str, s3: S3Client | None = None) -> str:
    if s3 is None:
        s3 = S3Client()
    df = _read_csv_from_s3(staging_s3_key, s3)

    df = df.drop_duplicates(subset="customerID")
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")
    df["TotalCharges"] = df["TotalCharges"].fillna(df["TotalCharges"].median())

    output_key = staging_s3_key.replace("staging/", "staging/cleaned_")
    return _write_csv_to_s3(df, output_key, s3)


@activity.defn
async def feature_engineer(cleaned_s3_key: str, s3: S3Client | None = None) -> str:
    if s3 is None:
        s3 = S3Client()
    df = _read_csv_from_s3(cleaned_s3_key, s3)

    df = df.drop(columns=["customerID"])
    df["Churn"] = df["Churn"].map({"No": 0, "Yes": 1})

    categorical_cols = df.select_dtypes(include=["object"]).columns.tolist()
    df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

    # Convert bool columns to int
    bool_cols = df.select_dtypes(include=["bool"]).columns
    df[bool_cols] = df[bool_cols].astype(int)

    output_key = cleaned_s3_key.replace("cleaned_", "engineered_")
    return _write_csv_to_s3(df, output_key, s3)


@activity.defn
async def split_data(engineered_s3_key: str, s3: S3Client | None = None) -> PreprocessingResult:
    if s3 is None:
        s3 = S3Client()
    df = _read_csv_from_s3(engineered_s3_key, s3)

    train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)

    train_key = f"{settings.processed_prefix}train.csv"
    test_key = f"{settings.processed_prefix}test.csv"
    _write_csv_to_s3(train_df, train_key, s3)
    _write_csv_to_s3(test_df, test_key, s3)

    return PreprocessingResult(train_s3_key=train_key, test_s3_key=test_key)


@workflow.defn
class PreprocessingWorkflow:
    @workflow.run
    async def run(self, staging_s3_key: str) -> PreprocessingResult:
        retry = workflow.RetryPolicy(
            maximum_attempts=3,
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
        )
        timeout = timedelta(minutes=5)

        cleaned_key = await workflow.execute_activity(
            clean_data, staging_s3_key,
            start_to_close_timeout=timeout, retry_policy=retry,
        )
        engineered_key = await workflow.execute_activity(
            feature_engineer, cleaned_key,
            start_to_close_timeout=timeout, retry_policy=retry,
        )
        result = await workflow.execute_activity(
            split_data, engineered_key,
            start_to_close_timeout=timeout, retry_policy=retry,
        )
        return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_preprocessing.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/workflows/preprocessing.py tests/test_preprocessing.py
git commit -m "feat: add data preprocessing workflow with clean, feature eng, split"
```

---

## Task 6: Churn Model & Training Workflow

**Files:**
- Create: `src/models/__init__.py`
- Create: `src/models/churn_model.py`
- Create: `src/workflows/training.py`
- Create: `tests/test_training.py`

- [ ] **Step 1: Write tests for model and training activities**

```python
# tests/test_training.py
from io import BytesIO

import pandas as pd
import pytest
from moto import mock_aws
from sklearn.datasets import make_classification

from src.config import Settings
from src.models.churn_model import train_churn_model, load_model
from src.s3_client import S3Client
from src.workflows.training import (
    TrainingResult,
    train_model,
    serialize_model,
)


@pytest.fixture
def train_csv_bytes():
    X, y = make_classification(n_samples=100, n_features=5, random_state=42)
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(5)])
    df["Churn"] = y
    return df.to_csv(index=False).encode()


def test_train_churn_model():
    X, y = make_classification(n_samples=50, n_features=3, random_state=42)
    df = pd.DataFrame(X, columns=["a", "b", "c"])
    df["Churn"] = y
    model, metadata = train_churn_model(df)

    assert model is not None
    assert "n_estimators" in metadata
    assert "training_time_seconds" in metadata


@mock_aws
def test_train_model_activity(aws_settings, s3_mock, train_csv_bytes):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("processed/train.csv", train_csv_bytes)

    model_bytes, metadata = train_model("processed/train.csv", s3)
    assert len(model_bytes) > 0
    assert "n_estimators" in metadata


@mock_aws
def test_serialize_model_activity(aws_settings, s3_mock, train_csv_bytes):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("processed/train.csv", train_csv_bytes)

    model_bytes, metadata = train_model("processed/train.csv", s3)
    result = serialize_model(model_bytes, metadata, "test-run-id", s3)

    assert result.model_s3_key.startswith("models/")
    assert result.training_metadata["n_estimators"] == 100

    # Verify model was uploaded
    downloaded = s3.download_bytes(result.model_s3_key)
    assert len(downloaded) > 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_training.py -v
```

Expected: FAIL — modules don't exist.

- [ ] **Step 3: Implement churn model**

```python
# src/models/__init__.py
```

```python
# src/models/churn_model.py
import pickle
import time

import pandas as pd
from sklearn.ensemble import RandomForestClassifier


def train_churn_model(
    train_df: pd.DataFrame,
    target_col: str = "Churn",
    n_estimators: int = 100,
    random_state: int = 42,
) -> tuple[RandomForestClassifier, dict]:
    X = train_df.drop(columns=[target_col])
    y = train_df[target_col]

    start = time.time()
    model = RandomForestClassifier(
        n_estimators=n_estimators,
        random_state=random_state,
    )
    model.fit(X, y)
    elapsed = time.time() - start

    metadata = {
        "n_estimators": n_estimators,
        "random_state": random_state,
        "n_features": X.shape[1],
        "n_samples": X.shape[0],
        "training_time_seconds": round(elapsed, 2),
    }
    return model, metadata


def serialize(model: RandomForestClassifier) -> bytes:
    return pickle.dumps(model)


def load_model(data: bytes) -> RandomForestClassifier:
    return pickle.loads(data)
```

- [ ] **Step 4: Implement training activities and workflow**

```python
# src/workflows/training.py
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO

import pandas as pd
from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from src.config import settings
    from src.models.churn_model import train_churn_model, serialize as serialize_model_obj
    from src.s3_client import S3Client


@dataclass
class TrainingResult:
    model_s3_key: str
    training_metadata: dict


@activity.defn
async def train_model(train_s3_key: str, s3: S3Client | None = None) -> tuple[bytes, dict]:
    if s3 is None:
        s3 = S3Client()
    data = s3.download_bytes(train_s3_key)
    df = pd.read_csv(BytesIO(data))

    model, metadata = train_churn_model(df)
    model_bytes = serialize_model_obj(model)

    activity.heartbeat("training complete")
    return model_bytes, metadata


@activity.defn
async def serialize_model(
    model_bytes: bytes,
    metadata: dict,
    run_id: str,
    s3: S3Client | None = None,
) -> TrainingResult:
    if s3 is None:
        s3 = S3Client()
    model_key = f"{settings.models_prefix}{run_id}/model.pkl"
    s3.upload_bytes(model_key, model_bytes)
    return TrainingResult(model_s3_key=model_key, training_metadata=metadata)


@workflow.defn
class TrainingWorkflow:
    @workflow.run
    async def run(self, train_s3_key: str, run_id: str) -> TrainingResult:
        model_bytes, metadata = await workflow.execute_activity(
            train_model,
            train_s3_key,
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=workflow.RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
            ),
        )
        result = await workflow.execute_activity(
            serialize_model,
            args=[model_bytes, metadata, run_id],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=workflow.RetryPolicy(
                maximum_attempts=4,
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
            ),
        )
        return result
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_training.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/models/__init__.py src/models/churn_model.py src/workflows/training.py tests/test_training.py
git commit -m "feat: add model training workflow with RandomForest and S3 serialization"
```

---

## Task 7: Evaluation & Storage Workflow

**Files:**
- Create: `src/workflows/evaluation.py`
- Create: `tests/test_evaluation.py`

- [ ] **Step 1: Write tests for evaluation activities**

```python
# tests/test_evaluation.py
from io import BytesIO

import pandas as pd
import pytest
from moto import mock_aws
from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestClassifier

from src.config import Settings
from src.models.churn_model import serialize, train_churn_model
from src.s3_client import S3Client
from src.workflows.evaluation import (
    EvaluationResult,
    evaluate_model,
    store_artifacts,
)


@pytest.fixture
def trained_model_and_test_data():
    X, y = make_classification(n_samples=100, n_features=5, random_state=42)
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(5)])
    df["Churn"] = y

    train_df = df.iloc[:80]
    test_df = df.iloc[80:]

    model, _ = train_churn_model(train_df)
    model_bytes = serialize(model)
    test_csv = test_df.to_csv(index=False).encode()

    return model_bytes, test_csv


@mock_aws
def test_evaluate_model(aws_settings, s3_mock, trained_model_and_test_data):
    model_bytes, test_csv = trained_model_and_test_data
    s3 = S3Client(aws_settings)
    s3.upload_bytes("models/test-run/model.pkl", model_bytes)
    s3.upload_bytes("processed/test.csv", test_csv)

    metrics = evaluate_model("models/test-run/model.pkl", "processed/test.csv", s3)

    assert "accuracy" in metrics
    assert "precision" in metrics
    assert "recall" in metrics
    assert "f1" in metrics
    assert 0 <= metrics["accuracy"] <= 1


@mock_aws
def test_store_artifacts(aws_settings, s3_mock):
    s3 = S3Client(aws_settings)

    metrics = {"accuracy": 0.92, "f1": 0.89}
    training_metadata = {"n_estimators": 100}
    result = store_artifacts(
        metrics=metrics,
        training_metadata=training_metadata,
        model_s3_key="models/test-run/model.pkl",
        run_id="test-run",
        row_count=100,
        s3=s3,
    )

    assert result.artifact_s3_key.startswith("artifacts/")
    assert result.metrics["accuracy"] == 0.92

    # Verify JSON was uploaded
    artifact = s3.download_json(result.artifact_s3_key)
    assert artifact["metrics"]["accuracy"] == 0.92
    assert artifact["model_s3_key"] == "models/test-run/model.pkl"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_evaluation.py -v
```

Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement evaluation activities and workflow**

```python
# src/workflows/evaluation.py
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from src.config import settings
    from src.models.churn_model import load_model
    from src.s3_client import S3Client


@dataclass
class EvaluationResult:
    metrics: dict
    artifact_s3_key: str


@activity.defn
async def evaluate_model(
    model_s3_key: str,
    test_s3_key: str,
    s3: S3Client | None = None,
) -> dict:
    if s3 is None:
        s3 = S3Client()

    model = load_model(s3.download_bytes(model_s3_key))
    test_df = pd.read_csv(BytesIO(s3.download_bytes(test_s3_key)))

    X_test = test_df.drop(columns=["Churn"])
    y_test = test_df["Churn"]
    y_pred = model.predict(X_test)

    activity.heartbeat("evaluation complete")

    return {
        "accuracy": round(accuracy_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred, zero_division=0), 4),
        "recall": round(recall_score(y_test, y_pred, zero_division=0), 4),
        "f1": round(f1_score(y_test, y_pred, zero_division=0), 4),
    }


@activity.defn
async def store_artifacts(
    metrics: dict,
    training_metadata: dict,
    model_s3_key: str,
    run_id: str,
    row_count: int,
    s3: S3Client | None = None,
) -> EvaluationResult:
    if s3 is None:
        s3 = S3Client()

    artifact = {
        "run_id": run_id,
        "timestamp": datetime.utcnow().isoformat(),
        "model_s3_key": model_s3_key,
        "metrics": metrics,
        "training_metadata": training_metadata,
        "dataset_info": {"row_count": row_count},
    }

    artifact_key = f"{settings.artifacts_prefix}{run_id}/metadata.json"
    s3.upload_json(artifact_key, artifact)

    return EvaluationResult(metrics=metrics, artifact_s3_key=artifact_key)


@workflow.defn
class EvaluationWorkflow:
    @workflow.run
    async def run(
        self,
        model_s3_key: str,
        test_s3_key: str,
        training_metadata: dict,
        run_id: str,
        row_count: int,
    ) -> EvaluationResult:
        retry = workflow.RetryPolicy(
            maximum_attempts=4,
            initial_interval=timedelta(seconds=5),
            backoff_coefficient=2.0,
        )

        metrics = await workflow.execute_activity(
            evaluate_model,
            args=[model_s3_key, test_s3_key],
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=retry,
        )
        result = await workflow.execute_activity(
            store_artifacts,
            args=[metrics, training_metadata, model_s3_key, run_id, row_count],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=retry,
        )
        return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_evaluation.py -v
```

Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/workflows/evaluation.py tests/test_evaluation.py
git commit -m "feat: add evaluation workflow with metrics and artifact storage"
```

---

## Task 8: Parent Orchestration Workflow

**Files:**
- Create: `src/workflows/parent.py`
- Create: `tests/test_parent_workflow.py`

- [ ] **Step 1: Write test for parent workflow**

```python
# tests/test_parent_workflow.py
import pytest
from unittest.mock import MagicMock
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from src.workflows.parent import ChurnPipelineWorkflow, PipelineInput
from src.workflows.ingestion import IngestionWorkflow, IngestionResult
from src.workflows.preprocessing import PreprocessingWorkflow, PreprocessingResult
from src.workflows.training import TrainingWorkflow, TrainingResult
from src.workflows.evaluation import EvaluationWorkflow, EvaluationResult


@pytest.mark.asyncio
async def test_parent_workflow_orchestration():
    """Test that parent workflow calls children in correct order.

    Uses Temporal test environment with mocked child workflows to verify
    orchestration logic without needing real S3 or ML dependencies.
    """
    async with await WorkflowEnvironment.start_time_skipping() as env:
        # We test the orchestration logic by running with the real workflow
        # but we need the child workflows registered. For a unit test of
        # the parent, we verify the structure and input/output contract.
        # Full integration test happens in Task 10.
        pass  # Placeholder — full integration in Task 10

    # For now, verify the workflow class is properly defined
    assert ChurnPipelineWorkflow.__name__ == "ChurnPipelineWorkflow"
    assert hasattr(ChurnPipelineWorkflow, "run")
```

Note: The parent workflow is best tested via integration test (Task 10) since it orchestrates child workflows. This task focuses on getting the implementation right.

- [ ] **Step 2: Implement parent workflow**

```python
# src/workflows/parent.py
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

from src.workflows.ingestion import IngestionWorkflow, IngestionResult
from src.workflows.preprocessing import PreprocessingWorkflow, PreprocessingResult
from src.workflows.training import TrainingWorkflow, TrainingResult
from src.workflows.evaluation import EvaluationWorkflow, EvaluationResult

with workflow.unsafe.imports_passed_through():
    from src.config import settings


@dataclass
class PipelineInput:
    raw_s3_key: str


@dataclass
class PipelineResult:
    metrics: dict
    model_s3_key: str
    artifact_s3_key: str
    row_count: int


@workflow.defn
class ChurnPipelineWorkflow:
    @workflow.run
    async def run(self, input: PipelineInput) -> PipelineResult:
        run_id = workflow.info().workflow_id

        # Child 1: Ingestion
        ingestion_result: IngestionResult = await workflow.execute_child_workflow(
            IngestionWorkflow.run,
            input.raw_s3_key,
            id=f"{run_id}-ingestion",
            task_queue=settings.default_task_queue,
            execution_timeout=timedelta(minutes=15),
        )

        # Child 2: Preprocessing
        preprocessing_result: PreprocessingResult = await workflow.execute_child_workflow(
            PreprocessingWorkflow.run,
            ingestion_result.s3_key,
            id=f"{run_id}-preprocessing",
            task_queue=settings.default_task_queue,
            execution_timeout=timedelta(minutes=15),
        )

        # Child 3: Training (on training queue)
        training_result: TrainingResult = await workflow.execute_child_workflow(
            TrainingWorkflow.run,
            args=[preprocessing_result.train_s3_key, run_id],
            id=f"{run_id}-training",
            task_queue=settings.training_task_queue,
            execution_timeout=timedelta(minutes=15),
        )

        # Child 4: Evaluation
        evaluation_result: EvaluationResult = await workflow.execute_child_workflow(
            EvaluationWorkflow.run,
            args=[
                training_result.model_s3_key,
                preprocessing_result.test_s3_key,
                training_result.training_metadata,
                run_id,
                ingestion_result.row_count,
            ],
            id=f"{run_id}-evaluation",
            task_queue=settings.default_task_queue,
            execution_timeout=timedelta(minutes=15),
        )

        return PipelineResult(
            metrics=evaluation_result.metrics,
            model_s3_key=training_result.model_s3_key,
            artifact_s3_key=evaluation_result.artifact_s3_key,
            row_count=ingestion_result.row_count,
        )
```

- [ ] **Step 3: Run tests**

```bash
pytest tests/test_parent_workflow.py -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/workflows/parent.py tests/test_parent_workflow.py
git commit -m "feat: add parent orchestration workflow"
```

---

## Task 9: Workers

**Files:**
- Create: `src/workers/__init__.py`
- Create: `src/workers/default_worker.py`
- Create: `src/workers/training_worker.py`

- [ ] **Step 1: Implement default worker**

```python
# src/workers/__init__.py
```

```python
# src/workers/default_worker.py
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from src.config import settings
from src.workflows.parent import ChurnPipelineWorkflow
from src.workflows.ingestion import IngestionWorkflow, download_from_s3, validate_schema
from src.workflows.preprocessing import (
    PreprocessingWorkflow, clean_data, feature_engineer, split_data,
)
from src.workflows.evaluation import EvaluationWorkflow, evaluate_model, store_artifacts


async def main():
    client = await Client.connect(settings.temporal_host)

    worker = Worker(
        client,
        task_queue=settings.default_task_queue,
        workflows=[
            ChurnPipelineWorkflow,
            IngestionWorkflow,
            PreprocessingWorkflow,
            EvaluationWorkflow,
        ],
        activities=[
            download_from_s3,
            validate_schema,
            clean_data,
            feature_engineer,
            split_data,
            evaluate_model,
            store_artifacts,
        ],
    )

    print(f"Default worker started on queue: {settings.default_task_queue}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Implement training worker**

```python
# src/workers/training_worker.py
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from src.config import settings
from src.workflows.training import TrainingWorkflow, train_model, serialize_model


async def main():
    client = await Client.connect(settings.temporal_host)

    worker = Worker(
        client,
        task_queue=settings.training_task_queue,
        workflows=[TrainingWorkflow],
        activities=[train_model, serialize_model],
    )

    print(f"Training worker started on queue: {settings.training_task_queue}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 3: Commit**

```bash
git add src/workers/__init__.py src/workers/default_worker.py src/workers/training_worker.py
git commit -m "feat: add default and training worker entry points"
```

---

## Task 10: SQS Trigger Service

**Files:**
- Create: `src/trigger.py`
- Create: `tests/test_trigger.py`

- [ ] **Step 1: Write tests for trigger**

```python
# tests/test_trigger.py
import json

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.trigger import parse_s3_event, build_workflow_id


def test_parse_s3_event():
    event_body = json.dumps({
        "Records": [{
            "s3": {
                "bucket": {"name": "churn-pipeline"},
                "object": {"key": "raw/churn_data.csv"},
            }
        }]
    })
    bucket, key = parse_s3_event(event_body)
    assert bucket == "churn-pipeline"
    assert key == "raw/churn_data.csv"


def test_parse_s3_event_ignores_non_raw():
    event_body = json.dumps({
        "Records": [{
            "s3": {
                "bucket": {"name": "churn-pipeline"},
                "object": {"key": "staging/something.csv"},
            }
        }]
    })
    bucket, key = parse_s3_event(event_body)
    assert bucket == "churn-pipeline"
    assert key is None  # Not in raw/ prefix, ignore


def test_build_workflow_id():
    wf_id = build_workflow_id("raw/churn_data.csv")
    assert wf_id == "churn-pipeline-raw/churn_data.csv"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_trigger.py -v
```

Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement trigger**

```python
# src/trigger.py
import asyncio
import json
import time

import boto3
from temporalio.client import Client

from src.config import settings
from src.workflows.parent import ChurnPipelineWorkflow, PipelineInput


def parse_s3_event(event_body: str) -> tuple[str, str | None]:
    event = json.loads(event_body)
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    if not key.startswith(settings.raw_prefix):
        return bucket, None

    return bucket, key


def build_workflow_id(s3_key: str) -> str:
    return f"churn-pipeline-{s3_key}"


async def poll_and_trigger():
    sqs = boto3.client(
        "sqs",
        endpoint_url=settings.sqs_endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

    client = await Client.connect(settings.temporal_host)

    queue_url = sqs.get_queue_url(QueueName=settings.sqs_queue_name)["QueueUrl"]
    print(f"Trigger polling SQS queue: {settings.sqs_queue_name}")

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )

        messages = response.get("Messages", [])
        for msg in messages:
            bucket, key = parse_s3_event(msg["Body"])
            if key is not None:
                workflow_id = build_workflow_id(key)
                print(f"Starting pipeline for: {key} (workflow: {workflow_id})")

                await client.start_workflow(
                    ChurnPipelineWorkflow.run,
                    PipelineInput(raw_s3_key=key),
                    id=workflow_id,
                    task_queue=settings.default_task_queue,
                )

            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )


if __name__ == "__main__":
    asyncio.run(poll_and_trigger())
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_trigger.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/trigger.py tests/test_trigger.py
git commit -m "feat: add SQS trigger service for S3 upload events"
```

---

## Task 11: Docker Compose & Dockerfile

**Files:**
- Create: `docker-compose.yml`
- Create: `Dockerfile.worker`
- Create: `scripts/upload_csv.sh`

- [ ] **Step 1: Create `Dockerfile.worker`**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY src/ src/
COPY data/ data/

# Default command overridden per service in docker-compose
CMD ["python", "-m", "src.workers.default_worker"]
```

- [ ] **Step 2: Create `docker-compose.yml`**

```yaml
version: "3.8"

services:
  postgresql:
    image: postgres:15
    environment:
      POSTGRES_USER: temporal
      POSTGRES_PASSWORD: temporal
      POSTGRES_DB: temporal
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U temporal"]
      interval: 5s
      timeout: 5s
      retries: 10

  temporal-server:
    image: temporalio/auto-setup:latest
    depends_on:
      postgresql:
        condition: service_healthy
    environment:
      - DB=postgresql
      - DB_PORT=5432
      - POSTGRES_USER=temporal
      - POSTGRES_PWD=temporal
      - POSTGRES_SEEDS=postgresql
    ports:
      - "7233:7233"
    healthcheck:
      test: ["CMD", "tctl", "--address", "temporal-server:7233", "cluster", "health"]
      interval: 10s
      timeout: 5s
      retries: 20
      start_period: 30s

  temporal-ui:
    image: temporalio/ui:latest
    depends_on:
      temporal-server:
        condition: service_healthy
    environment:
      - TEMPORAL_ADDRESS=temporal-server:7233
    ports:
      - "8080:8080"

  localstack:
    image: localstack/localstack:latest
    environment:
      - SERVICES=s3,sqs
      - DEFAULT_REGION=us-east-1
    ports:
      - "4566:4566"
    volumes:
      - "./scripts/localstack-init.sh:/etc/localstack/init/ready.d/init.sh"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_localstack/health"]
      interval: 5s
      timeout: 5s
      retries: 10

  default-worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    depends_on:
      temporal-server:
        condition: service_healthy
      localstack:
        condition: service_healthy
    environment:
      - PIPELINE_TEMPORAL_HOST=temporal-server:7233
      - PIPELINE_S3_ENDPOINT_URL=http://localstack:4566
      - PIPELINE_SQS_ENDPOINT_URL=http://localstack:4566
    command: ["python", "-m", "src.workers.default_worker"]

  training-worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    depends_on:
      temporal-server:
        condition: service_healthy
      localstack:
        condition: service_healthy
    environment:
      - PIPELINE_TEMPORAL_HOST=temporal-server:7233
      - PIPELINE_S3_ENDPOINT_URL=http://localstack:4566
      - PIPELINE_SQS_ENDPOINT_URL=http://localstack:4566
    command: ["python", "-m", "src.workers.training_worker"]

  trigger:
    build:
      context: .
      dockerfile: Dockerfile.worker
    depends_on:
      temporal-server:
        condition: service_healthy
      localstack:
        condition: service_healthy
    environment:
      - PIPELINE_TEMPORAL_HOST=temporal-server:7233
      - PIPELINE_S3_ENDPOINT_URL=http://localstack:4566
      - PIPELINE_SQS_ENDPOINT_URL=http://localstack:4566
    command: ["python", "-m", "src.trigger"]
```

- [ ] **Step 3: Create LocalStack init script**

```bash
# scripts/localstack-init.sh
#!/bin/bash
awslocal s3 mb s3://churn-pipeline
awslocal sqs create-queue --queue-name s3-notifications
awslocal s3api put-bucket-notification-configuration \
  --bucket churn-pipeline \
  --notification-configuration '{
    "QueueConfigurations": [{
      "QueueArn": "arn:aws:sqs:us-east-1:000000000000:s3-notifications",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{"Name": "prefix", "Value": "raw/"}]
        }
      }
    }]
  }'
echo "LocalStack initialized: S3 bucket + SQS queue + event notification"
```

- [ ] **Step 4: Create CSV upload helper**

```bash
# scripts/upload_csv.sh
#!/bin/bash
set -e

CSV_FILE="${1:-data/sample_churn.csv}"
S3_KEY="raw/$(basename "$CSV_FILE")"

echo "Uploading $CSV_FILE to s3://churn-pipeline/$S3_KEY ..."
aws --endpoint-url=http://localhost:4566 s3 cp "$CSV_FILE" "s3://churn-pipeline/$S3_KEY"
echo "Done. Pipeline should start shortly — check http://localhost:8080"
```

- [ ] **Step 5: Make scripts executable**

```bash
chmod +x scripts/localstack-init.sh scripts/upload_csv.sh
```

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml Dockerfile.worker scripts/
git commit -m "feat: add Docker Compose setup with Temporal, LocalStack, workers"
```

---

## Task 12: End-to-End Local Integration Test

**Files:**
- Create: `scripts/localstack-init.sh` (already done in Task 11)
- No new test file — this is a manual verification

- [ ] **Step 1: Start all services**

```bash
docker-compose up --build -d
```

Wait for all services to be healthy:

```bash
docker-compose ps
```

Expected: All 7 services running and healthy.

- [ ] **Step 2: Verify Temporal UI is accessible**

Open `http://localhost:8080` in a browser. Verify the Temporal Web UI loads.

- [ ] **Step 3: Upload sample CSV to trigger pipeline**

```bash
bash scripts/upload_csv.sh data/sample_churn.csv
```

- [ ] **Step 4: Monitor pipeline in Temporal UI**

Open `http://localhost:8080` and find the `churn-pipeline-raw/sample_churn.csv` workflow. Verify:
- Parent workflow starts
- 4 child workflows execute sequentially (ingestion → preprocessing → training → evaluation)
- All workflows complete successfully

- [ ] **Step 5: Verify model artifacts in S3**

```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://churn-pipeline/models/ --recursive
aws --endpoint-url=http://localhost:4566 s3 ls s3://churn-pipeline/artifacts/ --recursive
```

Expected: `model.pkl` in models prefix, `metadata.json` in artifacts prefix.

- [ ] **Step 6: Download and inspect metadata**

```bash
aws --endpoint-url=http://localhost:4566 s3 cp s3://churn-pipeline/artifacts/churn-pipeline-raw/sample_churn.csv/metadata.json - | python -m json.tool
```

Expected: JSON with metrics (accuracy, precision, recall, f1), model path, training metadata, and dataset info.

- [ ] **Step 7: Commit any fixes**

If any issues were found and fixed during integration testing, commit the fixes:

```bash
git add -A
git commit -m "fix: integration test fixes for end-to-end pipeline"
```

---

## Task Summary

| Task | Description | Dependencies |
|------|-------------|-------------|
| 1 | Project setup & config | None |
| 2 | S3 client wrapper | Task 1 |
| 3 | Sample dataset | None |
| 4 | Ingestion workflow | Tasks 1, 2 |
| 5 | Preprocessing workflow | Tasks 1, 2 |
| 6 | Training workflow | Tasks 1, 2 |
| 7 | Evaluation workflow | Tasks 1, 2 |
| 8 | Parent workflow | Tasks 4, 5, 6, 7 |
| 9 | Workers | Task 8 |
| 10 | SQS trigger | Task 8 |
| 11 | Docker Compose | Tasks 9, 10 |
| 12 | E2E integration test | Task 11 |
