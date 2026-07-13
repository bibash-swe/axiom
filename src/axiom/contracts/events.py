"""
Wire contracts for events written to workflow_outbox.payload.

Deliberately minimal: an outbox event is a dispatch signal, not a data
carrier. Postgres remains the single source of truth for workflow data —
a worker that claims a job re-reads workflow_type, input_data, and
everything else directly from workflow_states, rather than trusting a
copy embedded in the event itself. That's what keeps this system from
ever having two independently-drifting versions of the same fact.
"""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class WorkflowStartedEvent(BaseModel):
    """
    The only outbox event type Phase 1 produces. Written by Ingress inside
    the same transaction as the workflow_states row (see
    ingress/repository.py). The Relay never inspects this payload's fields
    — it's a dumb pipe, forwarding the raw JSON opaquely. The Worker, in
    Phase 3, is the only component that actually parses this back into a
    structured object, and only to learn which workflow_id to claim.

    extra="forbid": an unexpected field here is a bug worth failing loudly
    on, not silently ignoring — this is a wire contract, not a loose dict.

    When a second event type is needed (e.g. a cancellation-requested
    event in Phase 5), promote `event_type` into a real OutboxEventType
    StrEnum in contracts/enums.py, and use Pydantic's discriminated unions
    (Field(discriminator="event_type")) to dispatch between event models.
    Not done now — a single-member enum for one event type is vocabulary
    we don't need yet.
    """

    model_config = ConfigDict(extra="forbid")

    event_type: Literal["WORKFLOW_STARTED"] = "WORKFLOW_STARTED"
    workflow_id: UUID
