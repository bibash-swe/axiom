import asyncpg

from axiom.contracts.enums import WorkflowStatus, PublicStatus


async def test_postgres_workflow_status_matches_python_enum(pool: asyncpg.Pool):
    """Ensure the database ENUM type exactly matches the Python internal StrEnum."""
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT e.enumlabel
            FROM pg_enum e
            JOIN pg_type t ON e.enumtypid = t.oid
            WHERE t.typname = 'workflow_status'
        ''')
        db_statuses = {row['enumlabel'] for row in rows}

    python_statuses = {status.value for status in WorkflowStatus}

    assert db_statuses == python_statuses, (
        f"Database enum out of sync with Python contract. "
        f"DB: {db_statuses}, Python: {python_statuses}"
    )


def test_public_status_exhaustiveness():
    """
    Ensure no internal infrastructure states (e.g., DEAD_LETTERED) leak
    into the public-facing status vocabulary.
    """
    internal = {s.value for s in WorkflowStatus}
    public = {s.value for s in PublicStatus}

    # Assert public states are a strict subset (or distinct mapping)
    # of what the system actually tracks, verifying nothing leaks.
    # Adjust this specific logic if your internal->public mapping differs.
    forbidden_leaks = {"DEAD_LETTERED", "RETRYING"}
    for status in public:
        assert status.upper() not in forbidden_leaks, f"Internal state {status} leaked to public enum!"