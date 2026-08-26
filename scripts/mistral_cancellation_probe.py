"""Does Mistral stop generating — and stop billing — when we disconnect mid-stream?

Stage 1 (tests/worker/test_transport_cancellation.py) proves our own code
closes the socket, observed from the server's side. It structurally cannot
answer what the *provider* does afterwards. That is a property of someone
else's infrastructure and is only measurable against a real one.

The only client-visible cost oracle Mistral exposes is the rate-limit token
budget, which is an indirect proxy. So this script does not simply use it —
it validates it first, in two controls, and refuses to run the experiment if
either fails:

  CONTROL A — does the limiter debit ACTUAL tokens generated, or RESERVE
    max_tokens at admission? If it reserves, an aborted run and a full run
    debit identically no matter what the backend does, and the experiment
    would report "the provider kept generating" in every possible world.

  CONTROL B — is the counter a stable bucket, or a sliding window? If it
    slides, a raw before/after delta is contaminated by however long the
    measured run took, and any settle delay shorter than the full
    generation time biases the result toward a false positive: a backend
    that keeps generating debits those tokens as it generates them, over
    the following tens of seconds.

Only then:

  EXPERIMENT — alternating ABORTED/FULL pairs where both arms read the
    counter at an IDENTICAL wall-clock offset from run start, so sliding
    refill affects them equally and cancels out of the comparison. Each
    trial is gated on an OBSERVED-drained window rather than an assumed
    sufficient quiet period.

Run deliberately. It costs a few thousand tokens of a cheap model and takes
several minutes, and it is not meant for CI:

    MISTRAL_API_KEY=... uv run python scripts/mistral_cancellation_probe.py
    MISTRAL_API_KEY=... uv run python scripts/mistral_cancellation_probe.py --controls-only

Findings from the August 2026 run are recorded in docs/decisions.md #15,
including what this instrument can and cannot resolve. Provider
cancellation behaviour changes without announcement — re-run rather than
trusting a recorded result indefinitely.
"""

import argparse
import asyncio
import os
import statistics
import sys
import time

import httpx

API_KEY = os.environ.get("MISTRAL_API_KEY")
BASE_URL = "https://api.mistral.ai/v1/chat/completions"
MODEL = "mistral-small-latest"

COUNTER_HDR = "x-ratelimit-remaining-tokens-minute"
LIMIT_HDR = "x-ratelimit-limit-tokens-minute"
COST_HDR = "x-ratelimit-tokens-query-cost"

# Control A: large enough that "reserved" and "actual" cannot be confused.
RESERVE_PROBE_MAX_TOKENS = 2000
# Control B: enough samples on a fixed cadence to see the window's shape.
BUCKET_SAMPLES = 14
BUCKET_INTERVAL_SECONDS = 5.0

MAX_TOKENS = 600
ABORT_AFTER_CHUNKS = 8
MEASURE_AT_SECONDS = 15.0
DRAIN_EPSILON = 300
DRAIN_POLL_SECONDS = 10.0
DRAIN_MAX_POLLS = 14
PAIRS = 4

# Engineered to consume the whole max_tokens budget rather than stopping
# itself early — a natural early stop would confound FULL against ABORTED.
LONG_PROMPT = (
    "Count from 1 to 2000, writing each number on its own line, "
    "with no other commentary. Do not stop early."
)


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}


async def probe(client: httpx.AsyncClient) -> tuple[int, int, int, int]:
    """Fire one minimal known-cost request.

    Returns (remaining, limit, this_request_cost, actual_total_tokens). Its
    real purpose is the rate-limit headers, not its content.
    """
    resp = await client.post(
        BASE_URL,
        headers=_headers(),
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": "Say OK."}],
            "max_tokens": 5,
            "stream": False,
        },
        timeout=60.0,
    )
    resp.raise_for_status()
    h = {k.lower(): v for k, v in resp.headers.items()}
    usage = resp.json().get("usage") or {}
    return (
        int(h[COUNTER_HDR]),
        int(h[LIMIT_HDR]),
        int(h.get(COST_HDR, 0)),
        int(usage.get("total_tokens", 0)),
    )


async def control_a_meter_validity(client: httpx.AsyncClient) -> bool:
    """Check the limiter debits actual usage rather than reserving max_tokens.

    Sends a large max_tokens with a prompt that stops naturally after a
    couple of tokens, then compares the counter's movement against the
    response's own reported usage.
    """
    print("CONTROL A — does the limiter debit actual usage, or reserve max_tokens?")

    before, limit, _, _ = await probe(client)
    print(f"  counter {COUNTER_HDR} = {before}/{limit}")

    resp = await client.post(
        BASE_URL,
        headers=_headers(),
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": "Say OK. Nothing else."}],
            "max_tokens": RESERVE_PROBE_MAX_TOKENS,
            "stream": False,
        },
        timeout=60.0,
    )
    resp.raise_for_status()
    h = {k.lower(): v for k, v in resp.headers.items()}
    actual = int(resp.json()["usage"]["total_tokens"])
    after = int(h[COUNTER_HDR])
    decrement = before - after

    print(f"  requested max_tokens : {RESERVE_PROBE_MAX_TOKENS}")
    print(f"  tokens actually used : {actual}")
    print(f"  counter decrement    : {decrement}")

    near_actual = abs(decrement - actual) <= max(25, actual * 0.5)
    near_reserved = (
        abs(decrement - RESERVE_PROBE_MAX_TOKENS) <= RESERVE_PROBE_MAX_TOKENS * 0.25
    )

    if near_actual and not near_reserved:
        print("  PASS — debits actual usage. The budget-delta method is meaningful.\n")
        return True
    if near_reserved:
        print(
            "  FAIL — reserves max_tokens at admission. An aborted run and a full\n"
            "  run would debit identically regardless of backend behaviour, so this\n"
            "  method cannot answer the question. Aborting.\n"
        )
        return False
    print(
        f"  FAIL — decrement {decrement} matches neither actual usage ({actual}) nor\n"
        f"  reserved max_tokens ({RESERVE_PROBE_MAX_TOKENS}). The counter may be\n"
        "  request-denominated or shared across a tier. Aborting.\n"
    )
    return False


async def control_b_window_shape(client: httpx.AsyncClient) -> bool:
    """Determine whether the counter is a stable bucket or a sliding window.

    Fires identical known-cost requests on a fixed cadence. A counter that
    only debits falls by exactly the request cost each time; anything less
    means spending is being refunded mid-measurement.
    """
    print("CONTROL B — is the counter stable, or does it refill during measurement?")

    prev: int | None = None
    observed_debits: list[int] = []
    total_cost = 0

    for i in range(BUCKET_SAMPLES):
        remaining, limit, cost, _ = await probe(client)
        total_cost += cost
        if prev is not None:
            observed_debits.append(prev - remaining)
        if i < 3 or i == BUCKET_SAMPLES - 1:
            delta = "" if prev is None else f" (moved {remaining - prev:+d})"
            print(f"  sample {i:>2}: {remaining}/{limit} cost={cost}{delta}")
        prev = remaining
        if i < BUCKET_SAMPLES - 1:
            await asyncio.sleep(BUCKET_INTERVAL_SECONDS)

    mean_debit = statistics.mean(observed_debits) if observed_debits else 0.0
    mean_cost = total_cost / BUCKET_SAMPLES if BUCKET_SAMPLES else 0.0
    print(f"  mean charged cost per request : {mean_cost:.1f}")
    print(f"  mean observed counter movement: {mean_debit:.1f}")

    if mean_debit < mean_cost * 0.9:
        refund_rate = (mean_cost - mean_debit) / BUCKET_INTERVAL_SECONDS
        print(
            f"  SLIDING WINDOW confirmed — roughly {refund_rate:.1f} tokens/sec are\n"
            "  refunded as spending ages out. Two consequences the experiment must\n"
            "  handle, and does: both arms are read at an identical wall-clock\n"
            "  offset so refill cancels out, and each trial waits for an OBSERVED\n"
            "  drained window rather than a guessed quiet period.\n"
        )
    else:
        print(
            "  Counter behaves as a plain bucket over this span. The experiment's\n"
            "  symmetric-offset design is harmless but unnecessary here.\n"
        )
    return True


async def wait_for_drained_window(client: httpx.AsyncClient) -> tuple[int, bool]:
    """Poll until the counter sits within DRAIN_EPSILON of its limit.

    This is a precondition, not an assumption: it observes that no prior
    spending remains inside the window before a trial is measured.
    """
    for attempt in range(DRAIN_MAX_POLLS):
        remaining, limit, _, _ = await probe(client)
        if remaining >= limit - DRAIN_EPSILON:
            print(f"    window drained ({remaining}/{limit}) after {attempt} polls")
            return remaining, True
        if attempt == 0:
            print(f"    waiting for drain: {remaining}/{limit}, need >= {limit - DRAIN_EPSILON}")
        await asyncio.sleep(DRAIN_POLL_SECONDS)

    remaining, limit, _, _ = await probe(client)
    print(f"    WARNING: never drained ({remaining}/{limit}) — sample will be discarded")
    return remaining, False


async def generate(client: httpx.AsyncClient, *, abort_after: int | None) -> tuple[int, float]:
    """Stream a generation, disconnecting after abort_after chunks if set.

    Returns (chunks_received, seconds_connected). Leaving the `async with`
    closes the response, the same disconnect shape stream_guard produces.
    """
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": LONG_PROMPT}],
        "max_tokens": MAX_TOKENS,
        "stream": True,
    }
    chunks = 0
    started = time.monotonic()
    async with client.stream(
        "POST", BASE_URL, headers=_headers(), json=payload, timeout=None
    ) as resp:
        resp.raise_for_status()
        async for line in resp.aiter_lines():
            if line.startswith("data: ") and line != "data: [DONE]":
                chunks += 1
                if abort_after is not None and chunks >= abort_after:
                    break
    return chunks, time.monotonic() - started


async def trial(client: httpx.AsyncClient, *, arm: str) -> dict[str, object]:
    """Run one measured trial of either arm, gated on a drained window."""
    print(f"  [{arm}]")
    before, drained = await wait_for_drained_window(client)

    started = time.monotonic()
    chunks, connected = await generate(
        client, abort_after=ABORT_AFTER_CHUNKS if arm == "ABORTED" else None
    )
    await asyncio.sleep(max(0.0, MEASURE_AT_SECONDS - (time.monotonic() - started)))

    after, _, probe_cost, _ = await probe(client)
    consumed = before - after - probe_cost
    valid = drained and consumed >= 0

    note = "" if valid else "  <-- discarded"
    print(f"    chunks={chunks} connected={connected:.1f}s consumed={consumed}{note}")
    return {
        "arm": arm,
        "chunks": chunks,
        "connected": connected,
        "consumed": consumed,
        "valid": valid,
    }


def report(results: list[dict[str, object]]) -> None:
    """Print the comparison and state precisely what it does and does not establish."""
    full = [int(r["consumed"]) for r in results if r["arm"] == "FULL" and r["valid"]]
    abort_all = [int(r["consumed"]) for r in results if r["arm"] == "ABORTED"]
    abort_valid = [int(r["consumed"]) for r in results if r["arm"] == "ABORTED" and r["valid"]]

    print("\n" + "=" * 66)
    print(f"  FULL    (valid): {full}")
    print(f"  ABORTED (all)  : {abort_all}     valid subset: {abort_valid}")

    if len(full) < 2:
        print("\n  Too few valid FULL samples to conclude anything. Re-run.")
        return

    full_median = statistics.median(full)
    spread = (max(full) - min(full)) / full_median if full_median else 0.0
    print(f"\n  FULL median: {full_median}  (predicted ~{MAX_TOKENS + 25})")
    print(f"  FULL spread: {spread:.0%} of median")

    if spread > 0.25:
        print(
            f"\n  UNRELIABLE — FULL varies by {spread:.0%} across runs that should be\n"
            "  near-identical. The meter is not stable enough. Re-run."
        )
        return

    # The decisive comparison is not a ratio. If the backend ran to
    # max_tokens after disconnect, ABORTED would land near FULL — a signal
    # many times the noise floor. Whether it lands at 0 or at a small tail
    # is below this instrument's resolution either way.
    noise_floor = max(abs(v) for v in abort_all) if abort_all else 0
    continued = [v for v in abort_all if v > full_median * 0.7]

    print("\n--- VERDICT ---")
    if continued:
        print(
            f"  {len(continued)}/{len(abort_all)} ABORTED runs consumed near the FULL\n"
            "  cost. Mistral KEEPS GENERATING after client disconnect. max_tokens is\n"
            "  the real cost ceiling; closing the transport is a correctness\n"
            "  guarantee, NOT a cost-savings one."
        )
        return

    print(
        f"  No ABORTED run approached the FULL cost of ~{full_median}. Had generation\n"
        f"  continued to max_tokens, every ABORTED run would read near {full_median} —\n"
        "  a signal far above this meter's noise. It does not. Disconnecting DOES\n"
        "  prevent generation to max_tokens."
    )
    print(
        f"\n  NOT established: whether the residual is zero or a tail of a few dozen\n"
        f"  tokens. ABORTED consumption sits within +/-{noise_floor} of zero, which is\n"
        "  this instrument's noise floor. Resolving that needs a per-request usage\n"
        "  API, not a rate-limit header. Budget for a tail rather than assuming an\n"
        "  instant cutoff."
    )
    print(
        "\n  Scope: one provider, one model, one tier, one account, one day.\n"
        "  Re-run before relying on this."
    )


async def main(controls_only: bool) -> int:
    """Validate the instrument, then run the experiment if the controls pass."""
    if not API_KEY:
        print("Set MISTRAL_API_KEY before running.", file=sys.stderr)
        return 2

    async with httpx.AsyncClient() as client:
        if not await control_a_meter_validity(client):
            return 1
        if not await control_b_window_shape(client):
            return 1

        if controls_only:
            print("Controls passed. Stopping before the experiment as requested.")
            return 0

        print(f"EXPERIMENT — {PAIRS} alternating pairs\n")
        results: list[dict[str, object]] = []
        for i in range(PAIRS):
            print(f"=== pair {i + 1}/{PAIRS} ===")
            # ABORTED first: if it ran second, residual generation from the
            # FULL run could still be debiting into its measurement window.
            results.append(await trial(client, arm="ABORTED"))
            results.append(await trial(client, arm="FULL"))

    report(results)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--controls-only",
        action="store_true",
        help="validate the rate-limit meter and stop, without running the experiment",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.controls_only)))
