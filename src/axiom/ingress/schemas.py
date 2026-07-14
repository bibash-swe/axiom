"""HTTP-facing request/response shapes for the ingress gateway.

Deliberately separate from contracts/: nothing else internally consumes
the raw HTTP request or response shape, so per docs/decisions.md #2 these
don't belong in contracts/ — only the outbox event does.
"""

from typing import Any

from pydantic import BaseModel, Field, field_validator

from axiom.contracts.enums import PublicStatus


class WorkflowSubmitRequest(BaseModel):
    """POST /workflows request body."""

    workflow_type: str = Field(..., min_length=1, max_length=100)
    input_data: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = Field(..., min_length=1, max_length=255)
    workflow_version: str = Field(default="v1", max_length=20)

    @field_validator("workflow_type", "idempotency_key")
    @classmethod
    def no_blank_after_strip(cls, v: str) -> str:
        """Reject whitespace-only values that would otherwise pass min_length."""
        v = v.strip()
        if not v:
            raise ValueError("must not be blank")
        return v


class WorkflowSubmitResponse(BaseModel):
    """POST /workflows response body — status is always the public vocabulary, never internal."""

    id: str
    status: PublicStatus
    workflow_type: str
    workflow_version: str
    created_at: str
    replayed: bool