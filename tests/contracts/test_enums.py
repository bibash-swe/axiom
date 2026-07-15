"""Consistency tests for the status vocabulary.

Closes the two "known open items" listed in docs/decisions.md.
"""

import re

import asyncpg

from axiom.contracts.enums import INTERNAL_TO_PUBLIC_STATUS, WorkflowStatus


def test_internal_to_public_status_covers_every_workflow_status() -> None:
    """Every WorkflowStatus member must have exactly one entry in the public mapping.

    A tenth internal status added without updating the mapping is a bug
    worth catching here, not via a client-visible KeyError.
    """
    assert set(WorkflowStatus) == set(INTERNAL_TO_PUBLIC_STATUS.keys())


async def test_chk_status_constraint_matches_workflow_status_enum(pool: asyncpg.Pool) -> None:
    """Diff Postgres's own stored chk_status definition against WorkflowStatus directly.

    The constraint and the enum are two independently-authored
    representations of the same nine strings, in two different languages.
    Nothing else enforces they stay in sync — this is the same check run
    by hand during design, now made permanent.
    """
    async with pool.acquire() as conn:
        constraint_def = await conn.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'chk_status'"
        )

    assert constraint_def is not None, "chk_status constraint not found — migration not applied?"

    sql_values = set(re.findall(r"'([A-Z_]+)'::character varying", constraint_def))
    python_values = {s.value for s in WorkflowStatus}

    assert sql_values == python_values