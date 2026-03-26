# tests/test_trigger.py
import json

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
    assert key is None


def test_parse_s3_event_localstack_wrapped():
    """LocalStack wraps the S3 event in a Message key."""
    inner = json.dumps({
        "Records": [{
            "s3": {
                "bucket": {"name": "churn-pipeline"},
                "object": {"key": "raw/churn_data.csv"},
            }
        }]
    })
    event_body = json.dumps({"Message": inner})
    bucket, key = parse_s3_event(event_body)
    assert bucket == "churn-pipeline"
    assert key == "raw/churn_data.csv"


def test_parse_s3_event_no_records():
    event_body = json.dumps({"Type": "something_else"})
    bucket, key = parse_s3_event(event_body)
    assert key is None


def test_build_workflow_id():
    wf_id = build_workflow_id("raw/churn_data.csv")
    assert wf_id == "churn-pipeline-raw/churn_data.csv"
