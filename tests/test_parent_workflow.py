# tests/test_parent_workflow.py
from src.workflows.parent import ChurnPipelineWorkflow, PipelineInput, PipelineResult


def test_parent_workflow_class_defined():
    """Verify workflow class and dataclasses are properly defined."""
    assert ChurnPipelineWorkflow.__name__ == "ChurnPipelineWorkflow"
    assert hasattr(ChurnPipelineWorkflow, "run")

    inp = PipelineInput(raw_s3_key="raw/test.csv")
    assert inp.raw_s3_key == "raw/test.csv"

    result = PipelineResult(
        metrics={"accuracy": 0.9},
        model_s3_key="models/test/model.pkl",
        artifact_s3_key="artifacts/test/metadata.json",
        row_count=100,
    )
    assert result.metrics["accuracy"] == 0.9
