from typing import Any, TypedDict


class ModelTaskMessage(TypedDict):
    runId: str
    taskId: str
    sectionId: str
    bankId: str
    regionCode: str
    modelType: str
    payload: dict[str, Any]
    submittedAt: str
    traceId: str
    retryCount: int


class ModelResultMessage(TypedDict):
    runId: str
    taskId: str
    sectionId: str
    status: str
    riskLevel: int | None
    rawResult: dict[str, Any] | None
    artifactPath: str | None
    workerId: str
    startedAt: str
    completedAt: str | None
    durationMs: int | None
    errorMessage: str | None
