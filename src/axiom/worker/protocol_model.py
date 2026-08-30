"""Finite model of the claim/fence/settle/ack protocol.

Other tests sample an interleaving someone thought of; explore() enumerates all
of them for a bounded configuration. Transitions mirror the SQL in worker.py,
and test_protocol_model.py cross-checks them against a real Postgres.

Safety only — liveness would need a fairness assumption.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import NamedTuple

TERMINAL: frozenset[str] = frozenset({"COMPLETED", "FAILED", "DEAD_LETTERED"})

MANY_SETTLERS = 2


class Phase(StrEnum):
    """Where a worker is with one message."""

    IDLE = "IDLE"
    HOLDING = "HOLDING"
    SETTLED = "SETTLED"
    DONE = "DONE"

    # Gave up on this delivery: crashed, or found itself fenced. Cannot retry it
    # — process_message returns without acking, so the message sits in this
    # consumer's PEL until XAUTOCLAIM hands it to someone else.
    STOPPED = "STOPPED"


class Worker(NamedTuple):
    """A worker's own view, which may be stale."""

    phase: Phase
    generation: int


@dataclass(frozen=True)
class State:
    """Workflow row, workers, and stream. Hashable, so it can key the visited set."""

    status: str
    generation: int
    lease_expires_at: int
    workers: tuple[Worker, ...]
    now: int
    delivered_to: int | None
    acked: bool
    delivered_at: int

    # History variables: both properties they support are about transitions,
    # which an invariant over a single state cannot see.
    was_terminal: bool
    settlers: int

    def is_terminal(self) -> bool:
        """Row has reached a terminal status."""
        return self.status in TERMINAL


class Config(NamedTuple):
    """Bounds that keep the state space finite."""

    workers: int = 2
    lease_duration: int = 2
    xautoclaim_idle: int = 3
    max_generation: int = 3
    max_time: int = 5


def initial_state(config: Config) -> State:
    """Submitted and dispatched, not yet delivered."""
    return State(
        status="PENDING",
        generation=0,
        lease_expires_at=0,
        workers=tuple(Worker(Phase.IDLE, 0) for _ in range(config.workers)),
        now=0,
        delivered_to=None,
        acked=False,
        delivered_at=0,
        was_terminal=False,
        settlers=0,
    )


def with_worker(state: State, index: int, worker: Worker) -> State:
    """Replace one worker, leaving everything else alone."""
    workers = list(state.workers)
    workers[index] = worker
    return replace(state, workers=tuple(workers))


def settled_by(state: State, index: int, status: str) -> State:
    """Record a successful terminal write.

    settlers saturates at MANY_SETTLERS: the invariant only distinguishes none,
    one, and more than one, and an unbounded counter would make the space
    infinite for any mutation that settles repeatedly.
    """
    return with_worker(
        replace(
            state,
            status=status,
            was_terminal=True,
            settlers=min(state.settlers + 1, MANY_SETTLERS),
        ),
        index,
        Worker(Phase.SETTLED, state.workers[index].generation),
    )


def successors(state: State, config: Config) -> list[tuple[str, State]]:
    """Enabled actions as (label, next_state)."""
    out: list[tuple[str, State]] = []

    if state.delivered_to is None and not state.acked:
        for i, worker in enumerate(state.workers):
            if worker.phase is Phase.IDLE:
                delivered = replace(state, delivered_to=i, delivered_at=state.now)
                out.append((f"deliver->w{i}", delivered))

    # XAUTOCLAIM never tells the previous holder, which is why fencing exists.
    if (
        state.delivered_to is not None
        and not state.acked
        and state.now - state.delivered_at >= config.xautoclaim_idle
    ):
        for i, worker in enumerate(state.workers):
            if i != state.delivered_to and worker.phase is Phase.IDLE:
                stolen = replace(state, delivered_to=i, delivered_at=state.now)
                out.append((f"xautoclaim w{state.delivered_to}->w{i}", stolen))

    for i, worker in enumerate(state.workers):
        claimable = state.status == "PENDING" or (
            state.status == "RUNNING" and state.lease_expires_at < state.now
        )
        if (
            state.delivered_to == i
            and worker.phase is Phase.IDLE
            and claimable
            and state.generation < config.max_generation
        ):
            generation = state.generation + 1
            claimed = with_worker(
                replace(
                    state,
                    status="RUNNING",
                    generation=generation,
                    lease_expires_at=state.now + config.lease_duration,
                ),
                i,
                Worker(Phase.HOLDING, generation),
            )
            out.append((f"w{i} claim (gen {generation})", claimed))

        # Claim returned None. Acking is only safe if the workflow is finished;
        # a live lease means someone else is still running it.
        if state.delivered_to == i and worker.phase is Phase.IDLE and not claimable:
            if state.is_terminal():
                conceded = with_worker(
                    replace(state, acked=True, delivered_to=None), i, Worker(Phase.DONE, 0)
                )
                out.append((f"w{i} claim failed on a settled row, acks", conceded))
            else:
                held = with_worker(state, i, Worker(Phase.STOPPED, 0))
                out.append((f"w{i} claim failed on a live lease, leaves unacked", held))

        if worker.phase is Phase.HOLDING:
            if worker.generation == state.generation:
                renewed = replace(state, lease_expires_at=state.now + config.lease_duration)
                out.append((f"w{i} heartbeat", renewed))
                for status in ("COMPLETED", "FAILED"):
                    out.append((f"w{i} settle {status}", settled_by(state, i, status)))
            else:
                # Renewal and settle both match zero rows; the worker stops.
                abandoned = with_worker(state, i, Worker(Phase.STOPPED, 0))
                out.append((f"w{i} detects fencing", abandoned))
                out.append((f"w{i} settle fenced out (no-op)", abandoned))

        if worker.phase is Phase.SETTLED and state.delivered_to == i:
            acked = with_worker(
                replace(state, acked=True, delivered_to=None), i, Worker(Phase.DONE, 0)
            )
            out.append((f"w{i} ack", acked))

        if worker.phase in (Phase.HOLDING, Phase.SETTLED):
            out.append((f"w{i} crash", with_worker(state, i, Worker(Phase.STOPPED, 0))))

    if state.now < config.max_time:
        out.append(("tick", replace(state, now=state.now + 1)))

    return out


class Violation(NamedTuple):
    """A failed invariant with the shortest trace that reaches it."""

    invariant: str
    state: State
    trace: list[str]

    def render(self) -> str:
        """Counterexample, formatted for a test failure."""
        steps = "\n".join(f"    {i + 1:>2}. {step}" for i, step in enumerate(self.trace))
        return (
            f"invariant violated: {self.invariant}\n"
            f"  state: status={self.state.status} gen={self.state.generation} "
            f"acked={self.state.acked} settlers={self.state.settlers} "
            f"workers={list(self.state.workers)}\n"
            f"  trace ({len(self.trace)} steps):\n{steps}"
        )


def at_most_one_live_owner(state: State) -> bool:
    """Two workers may run at once; only one may hold the current generation."""
    owners = [
        w
        for w in state.workers
        if w.phase in (Phase.HOLDING, Phase.SETTLED) and w.generation == state.generation
    ]
    return len(owners) <= 1


def terminal_is_absorbing(state: State) -> bool:
    """Once settled, always settled."""
    return not state.was_terminal or state.is_terminal()


def settled_at_most_once(state: State) -> bool:
    """No workflow gets two successful terminal writes."""
    return state.settlers <= 1


def ack_implies_terminal(state: State) -> bool:
    """Last-In-Chain. Breaking this loses work silently: message gone, row unfinished."""
    return not state.acked or state.is_terminal()


Invariant = Callable[[State], bool]
Successors = Callable[[State, Config], list[tuple[str, State]]]

INVARIANTS: dict[str, Invariant] = {
    "at_most_one_live_owner": at_most_one_live_owner,
    "terminal_is_absorbing": terminal_is_absorbing,
    "settled_at_most_once": settled_at_most_once,
    "ack_implies_terminal": ack_implies_terminal,
}


def explore(
    config: Config,
    invariants: dict[str, Invariant] | None = None,
    successor_fn: Successors | None = None,
    stop_on_first_violation: bool = False,
) -> tuple[int, list[Violation]]:
    """BFS over every reachable state. Returns (states seen, violations).

    successor_fn lets the tests feed in deliberately broken protocols; a checker
    that has never reported a violation is not known to be able to.
    """
    checks = INVARIANTS if invariants is None else invariants
    step = successors if successor_fn is None else successor_fn

    start = initial_state(config)
    seen: set[State] = {start}
    queue: deque[tuple[State, list[str]]] = deque([(start, [])])
    violations: list[Violation] = []

    while queue:
        state, trace = queue.popleft()

        for name, predicate in checks.items():
            if not predicate(state):
                violations.append(Violation(name, state, trace))
                if stop_on_first_violation:
                    return len(seen), violations

        for label, nxt in step(state, config):
            if nxt not in seen:
                seen.add(nxt)
                queue.append((nxt, [*trace, label]))

    return len(seen), violations
