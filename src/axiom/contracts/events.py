"""Wire contracts for workflow_outbox payloads.

Payloads are dispatch signals, not redundant data carriers (see docs/decisions.md #9).
"""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class WorkflowStartedEvent(BaseModel):
    """Written by Ingress inside the workflow_states transaction.

    Strict payload validation via extra="forbid" ensures wire contract integrity.
    """

    model_config = ConfigDict(extra="forbid")

    event_type: Literal["WORKFLOW_STARTED"] = "WORKFLOW_STARTED"
    workflow_id: UUID
