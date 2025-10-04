from pydantic import BaseModel
from typing import Any, Dict, List
from enum import Enum


# === Source Type Enum ===
class SourceType(str, Enum):
    LOCAL = "local"
    S3 = "s3"


class StatusEnum(str, Enum):
    SUCCESS = "1"
    FAILED = "2"
    SKIPPED = "3"
    PROCESSING = "4"


class ApiUrl(str, Enum):
    workflow_filter = "https://dev-datahub.dksh.b2b.com.my/api/workflow/filter"
    workflow_session_start = "http://java-server:8080/api/target-endpoint"
    workflow_session_finish = "http://java-server:8080/api/target-endpoint"
    workflow_step_start = "http://java-server:8080/api/target-endpoint"
    workflow_step_finish = "http://java-server:8080/api/target-endpoint"


class StopTaskRequest(BaseModel):
    task_id: str
    reason: str | None = None


class FilePathRequest(BaseModel):
    file_path: str


class WorkflowStep(BaseModel):
    workflowStepId: str
    stepName: str
    stepOrder: int
    stepConfiguration: List[dict] = []


class WorkflowModel(BaseModel):
    id: str
    name: str
    workflowSteps: List[WorkflowStep]


class WorkflowSession(BaseModel):
    id: str
    status: str