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
