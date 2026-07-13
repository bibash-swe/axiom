"""
The internal workflow state machine vocabulary, and the public-facing
mapping derived from it.

This is the single Python source of truth for status values. The Postgres
CHECK constraint in migrations/001_initial_schema.sql must list the exact
same members — nothing currently enforces that automatically, which is a
real drift risk worth closing with a consistency test once the migration
exists (see the accompanying design notes).

Deliberately zero third-party imports. A bug in some other contract file's
Pydantic model must never be able to prevent importing this one — see
docs/decisions.md for the reasoning.
"""

from enum import StrEnum


class WorkflowStatus(StrEnum):
    """
    Every member uses an explicit string literal, never auto(). The stored
    value must never change as a side effect of renaming a Python
    identifier — auto() on StrEnum lowercases the member name by default,
    which would also silently disagree with the uppercase convention this
    entire system already uses in SQL and JSON.
    """

    # --- Non-terminal ------------------------------------------------------
    PENDING = "PENDING"  # written, outbox event created, not yet dispatched
    RUNNING = "RUNNING"  # claimed by a worker, lease held
    WAITING_FOR_INPUT = (
        "WAITING_FOR_INPUT"  # parked for HITL; lease released, message already ack'd
    )
    CANCELING = "CANCELING"  # cancellation requested, not yet confirmed stopped

    # --- Terminal ------------------------------------------------------------
    COMPLETED = "COMPLETED"  # succeeded
    FAILED = "FAILED"  # unrecoverable application-level error
    CANCELED = "CANCELED"  # stopped at user request — kept distinct from FAILED
    # so cancellation never corrupts a failure-rate signal
    DEAD_LETTERED = "DEAD_LETTERED"  # lease_generation exceeded max retries
    DISPATCH_FAILED = "DISPATCH_FAILED"  # Relay exhausted retry_count before a successful XADD


class PublicStatus(StrEnum):
    """
    The only vocabulary ever returned to a client. A real enum, not a bare
    str type, specifically so strict mypy can catch a typo in a future API
    handler — otherwise the strict = true setting in pyproject.toml buys us
    nothing at this exact boundary.

    DEAD_LETTERED and DISPATCH_FAILED are both intentionally invisible here:
    they're infrastructure-specific failure modes a client has no
    actionable use for, only that the workflow failed.
    """

    QUEUED = "queued"
    PROCESSING = "processing"
    WAITING_FOR_INPUT = "waiting_for_input"
    CANCELING = "canceling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


# Exhaustive by construction: every WorkflowStatus member has exactly one
# entry. If a tenth internal status is ever added without updating this
# dict, that's a bug worth catching at test time, not discovering via a
# client-visible KeyError in production — worth a dedicated exhaustiveness
# test once tests/contracts/ exists.
INTERNAL_TO_PUBLIC_STATUS: dict[WorkflowStatus, PublicStatus] = {
    WorkflowStatus.PENDING: PublicStatus.QUEUED,
    WorkflowStatus.RUNNING: PublicStatus.PROCESSING,
    WorkflowStatus.WAITING_FOR_INPUT: PublicStatus.WAITING_FOR_INPUT,
    WorkflowStatus.CANCELING: PublicStatus.CANCELING,
    WorkflowStatus.COMPLETED: PublicStatus.SUCCEEDED,
    WorkflowStatus.FAILED: PublicStatus.FAILED,
    WorkflowStatus.CANCELED: PublicStatus.CANCELED,
    WorkflowStatus.DEAD_LETTERED: PublicStatus.FAILED,
    WorkflowStatus.DISPATCH_FAILED: PublicStatus.FAILED,
}
