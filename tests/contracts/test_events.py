"""Regression tests for WorkflowStartedEvent's wire-contract strictness.

Each of these guards a property that a careless refactor could silently
loosen without anything else noticing — not general Pydantic behavior,
just the specific guarantees this contract depends on.
"""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from axiom.contracts.events import WorkflowStartedEvent


def test_valid_construction_round_trips() -> None:
    """A well-formed event constructs cleanly and round-trips its fields."""
    workflow_id = uuid4()
    event = WorkflowStartedEvent(workflow_id=workflow_id)

    assert event.event_type == "WORKFLOW_STARTED"
    assert event.workflow_id == workflow_id


def test_rejects_extra_fields() -> None:
    """extra="forbid" — an unexpected field is a bug worth failing loudly on."""
    with pytest.raises(ValidationError):
        WorkflowStartedEvent(workflow_id=uuid4(), sneaky_field="oops")  # type: ignore[call-arg]


def test_rejects_malformed_uuid() -> None:
    """workflow_id: UUID, not str, catches a malformed id at construction time."""
    with pytest.raises(ValidationError):
        WorkflowStartedEvent(workflow_id="not-a-uuid")  # type: ignore[arg-type]


def test_rejects_wrong_event_type() -> None:
    """event_type is a Literal — the only value allowed until a second event type exists."""
    with pytest.raises(ValidationError):
        WorkflowStartedEvent(workflow_id=uuid4(), event_type="SOMETHING_ELSE")  # type: ignore[arg-type]