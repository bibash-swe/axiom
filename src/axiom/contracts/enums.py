"""Internal workflow state vocabulary and public API mapping.

See docs/decisions.md for boundary enforcement reasoning.
"""

from enum import StrEnum


class WorkflowStatus(StrEnum):
    """Workflow state vocabulary.

    Values are wire contracts stored verbatim in Postgres — never auto();
    see docs/decisions.md #6.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_FOR_INPUT = "WAITING_FOR_INPUT"
    CANCELING = "CANCELING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    DEAD_LETTERED = "DEAD_LETTERED"
    DISPATCH_FAILED = "DISPATCH_FAILED"


class PublicStatus(StrEnum):
    """Client-facing state vocabulary.

    Excludes internal infrastructure failure modes like DEAD_LETTERED.
    """

    QUEUED = "queued"
    PROCESSING = "processing"
    WAITING_FOR_INPUT = "waiting_for_input"
    CANCELING = "canceling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


# Exhaustive mapping. See docs/decisions.md #7 for omitted states.
INTERNAL_TO_PUBLIC_STATUS: dict[WorkflowStatus, PublicStatus] = {
    WorkflowStatus.PENDING: PublicStatus.QUEUED,
    WorkflowStatus.RUNNING: PublicStatus.PROCESSING,
    WorkflowStatus.WAITING_FOR_INPUT: PublicStatus.WAITING_FOR_INPUT,
    WorkflowStatus.CANCELING: PublicStatus.CANCELING,
    WorkflowStatus.COMPLETED: PublicStatus.SUCCEEDED,
    WorkflowStatus.FAILED: PublicStatus.FAILED,
    WorkflowStatus.CANCELED: PublicStatus.CANCELED,
    WorkflowStatus.DISPATCH_FAILED: PublicStatus.FAILED,
    WorkflowStatus.DEAD_LETTERED: PublicStatus.FAILED,
}
