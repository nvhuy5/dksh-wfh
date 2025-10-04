from typing import Dict, Any, Callable, Optional

MASTER_DATA_LOAD_DATA_BODY = {
    "fileName": str,
    "loadMode": "INSERT",
    "data": Any
}

WORKFLOW_FILTER_BODY = {
    "filePath": str,
    "fileName": str,
    "fileExtension": str,
    "project": str,
    "source": str,
}

WORKFLOW_SESSION_BODY = {
    "workflowId": str,
    "celeryId": str,
    "filePath": str,
}
