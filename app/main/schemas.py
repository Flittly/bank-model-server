from typing import Any

from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    model_api: str = Field(
        ..., description="Legacy model API such as /v0/mi/risk-level"
    )
    payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = Field(default=600, ge=1, le=3600)


class TaskRunRequest(BaseModel):
    timeout_seconds: int = Field(default=600, ge=1, le=3600)
