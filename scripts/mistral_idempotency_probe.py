"""Does Mistral honour an idempotency key, so a replayed call is not re-billed?

This is the measurement behind the last open item in the README's cost-safety
section: a reclaimed workflow re-runs its handler from the start (decisions.md
#13), so a stalled-then-reclaimed workflow can pay a provider twice. Fencing
cannot prevent that — the first call was already issued and already billed
before anyone was superseded. The fix the README names is provider-side
idempotency keys, and whether those exist is a fact about someone else's
infrastructure.

Neither Mistral's API reference nor its chat-completions docs mention
idempotency, deduplication, or retry-safety. Absence from documentation is not
absence of behaviour, so this measures rather than concludes.

THE ORACLE, AND WHY IT IS NOT THE OBVIOUS ONE.
"Same text came back, so it was a cached replay" is wrong twice over: Mistral
exposes a `random_seed` parameter, and a low temperature can make two genuine
generations identical anyway. Either would report a cache hit in a world with
no cache. So the oracle is the response `id` plus
`x-ratelimit-tokens-query-cost`, which #15 established equals
`usage.total_tokens` exactly — a real per-request cost signal, not the sliding
window counter that made the first cancellation probe unreliable.

CONTROLS, RUN BEFORE THE EXPERIMENT IS TRUSTED.

  A — does `id` discriminate at all? Two identical requests with no key must
      come back with different ids. If they do not, the oracle is blind and
      every later comparison is meaningless.

  B — is the cost header still per-request truth? Re-checked on every call
      against usage.total_tokens, because #15 is three days old and providers
      change without announcement.

  C — is any observed sameness caused by the *key*? The same body with two
      DIFFERENT keys must regenerate. Without this, body-level caching would
      be indistinguishable from working idempotency.

  D — are unknown headers simply ignored? A nonsense header sent twice
      establishes the baseline for "this header did nothing", so a null result
      in the experiment can be read as "not supported" rather than "we sent it
      wrong".

Run deliberately. It costs a few hundred tokens of a cheap model:

    MISTRAL_API_KEY=... uv run python scripts/mistral_idempotency_probe.py

Findings from the August 2026 run are recorded in docs/decisions.md #18.
"""

import asyncio
import os
import sys
import uuid
from dataclasses import dataclass

import httpx

# Defaults to Mistral, the provider we hold a key for. The controls below make
# no Mistral-specific assumption beyond the cost header, so pointing this at any
# OpenAI-compatible endpoint measures that provider instead — xAI and OpenAI
# both expose the same request shape:
#
#   PROBE_BASE_URL=https://api.x.ai/v1 PROBE_MODEL=grok-4 PROBE_API_KEY=... \
#       uv run python scripts/mistral_idempotency_probe.py
#
# A provider that does not send a per-request cost header simply fails control
# B, which downgrades the result to the id comparison alone rather than
# invalidating it.
API_KEY = os.environ.get("PROBE_API_KEY") or os.environ.get("MISTRAL_API_KEY")
BASE_URL = os.environ.get("PROBE_BASE_URL", "https://api.mistral.ai/v1")
URL = f"{BASE_URL.rstrip('/')}/chat/completions"
MODEL = os.environ.get("PROBE_MODEL", "mistral-small-latest")
COST_HDR = "x-ratelimit-tokens-query-cost"

# High temperature and an open-ended prompt: two genuine generations should
# differ. If they do not, control A catches it and the run aborts.
PROMPT = "Write one surprising sentence about the deep ocean."
MAX_TOKENS = 40
TEMPERATURE = 1.0

PAUSE_SECONDS = 1.0


@dataclass
class Call:
    """One completion request and the facts needed to compare it with another."""

    status: int
    id: str | None
    text: str
    total_tokens: int | None
    header_cost: int | None
    body: str

    @property
    def cost_agrees(self) -> bool:
        """Control B, evaluated per call: does the header match reported usage?"""
        return self.header_cost is not None and self.header_cost == self.total_tokens


async def call(
    client: httpx.AsyncClient, *, extra_headers: dict[str, str] | None = None
) -> Call:
    """Issue one identical completion, varying only the headers under test."""
    response = await client.post(
        URL,
        headers=extra_headers or {},
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": PROMPT}],
            "max_tokens": MAX_TOKENS,
            "temperature": TEMPERATURE,
        },
    )
    raw_cost = response.headers.get(COST_HDR)
    try:
        payload = response.json()
    except ValueError:
        payload = {}

    usage = payload.get("usage") or {}
    choices = payload.get("choices") or [{}]
    return Call(
        status=response.status_code,
        id=payload.get("id"),
        text=(choices[0].get("message") or {}).get("content", "") or "",
        total_tokens=usage.get("total_tokens"),
        header_cost=int(raw_cost) if raw_cost is not None else None,
        body=response.text[:200],
    )


def show(label: str, c: Call) -> None:
    """Print one call's identifying facts."""
    print(
        f"    {label:<10} status={c.status}  id={(c.id or '-')[:12]}  "
        f"tokens={c.total_tokens}  header_cost={c.header_cost}"
    )
    if c.status != 200:
        print(f"               body: {c.body}")
    else:
        print(f"               text: {c.text.strip()[:88]!r}")


async def pair(
    client: httpx.AsyncClient, headers_first: dict[str, str], headers_second: dict[str, str]
) -> tuple[Call, Call]:
    """Two identical bodies, back to back, differing only in headers."""
    first = await call(client, extra_headers=headers_first)
    await asyncio.sleep(PAUSE_SECONDS)
    second = await call(client, extra_headers=headers_second)
    await asyncio.sleep(PAUSE_SECONDS)
    return first, second


def replayed(first: Call, second: Call) -> bool:
    """Did the second call return the first one's response instead of generating?"""
    return (
        first.status == 200
        and second.status == 200
        and first.id is not None
        and first.id == second.id
    )


async def main() -> int:
    """Run the controls, then the experiment, and report what was established."""
    if not API_KEY:
        print("set MISTRAL_API_KEY", file=sys.stderr)
        return 2

    async with httpx.AsyncClient(
        timeout=60, headers={"Authorization": f"Bearer {API_KEY}"}
    ) as client:
        every: list[Call] = []

        print("CONTROL A — two identical requests, no key: does `id` discriminate?")
        a1, a2 = await pair(client, {}, {})
        every += [a1, a2]
        show("first", a1)
        show("second", a2)
        if a1.status != 200 or a2.status != 200:
            print("\n  ABORT: baseline request failed; nothing below would mean anything.")
            return 1
        if replayed(a1, a2):
            print(
                "\n  ABORT: two independent requests share an id. The oracle is blind —"
                "\n  every comparison below would report a cache hit in a world with no cache."
            )
            return 1
        print("  PASS: distinct ids, so `id` can tell a replay from a regeneration.\n")

        print("CONTROL B — is the cost header still per-request truth?")
        disagreements = [c for c in every if not c.cost_agrees]
        if disagreements:
            print(
                f"  WARNING: {len(disagreements)}/{len(every)} calls had "
                f"{COST_HDR} != usage.total_tokens."
            )
            print("  The cost signal is unreliable; id comparison carries the result alone.")
        else:
            print(f"  PASS: {COST_HDR} == usage.total_tokens on every call so far.\n")

        print("CONTROL D — is an unknown header simply ignored?")
        nonsense = {"X-Axiom-Nonsense-Key": str(uuid.uuid4())}
        d1, d2 = await pair(client, nonsense, nonsense)
        every += [d1, d2]
        show("first", d1)
        show("second", d2)
        unknown_header_ignored = d1.status == 200 and d2.status == 200 and not replayed(d1, d2)
        print(
            "  baseline: unknown headers are accepted and ignored.\n"
            if unknown_header_ignored
            else "  note: the unknown header changed the outcome — read the experiment carefully.\n"
        )

        print("EXPERIMENT — the same idempotency key, twice.")
        results: dict[str, bool] = {}
        for header_name in ("Idempotency-Key", "X-Idempotency-Key"):
            key = {header_name: str(uuid.uuid4())}
            e1, e2 = await pair(client, key, key)
            every += [e1, e2]
            print(f"  {header_name}:")
            show("first", e1)
            show("second", e2)
            hit = replayed(e1, e2)
            results[header_name] = hit
            print(
                "    -> REPLAYED: same id, second call not regenerated"
                if hit
                else "    -> regenerated: different ids, so the key changed nothing"
            )
            if e2.status == 409:
                print("    -> 409 conflict: the key was recognised but the request was rejected")
            print()

        honoured = [name for name, hit in results.items() if hit]

        if honoured:
            print("CONTROL C — same body, DIFFERENT keys: was the key really the cause?")
            name = honoured[0]
            c1, c2 = await pair(
                client, {name: str(uuid.uuid4())}, {name: str(uuid.uuid4())}
            )
            every += [c1, c2]
            show("first", c1)
            show("second", c2)
            if replayed(c1, c2):
                print(
                    "\n  FAILED: different keys still returned one response. Whatever caused"
                    "\n  the match above, it was not the idempotency key — most likely"
                    "\n  body-level caching. The experiment does not support the conclusion."
                )
                return 1
            print("  PASS: different keys regenerate, so the key is what did it.\n")

        print("=" * 72)
        bad_cost = [c for c in every if c.status == 200 and not c.cost_agrees]
        print(f"{len(every)} calls, {sum(c.total_tokens or 0 for c in every)} tokens spent")
        print(f"cost header agreed with usage on {len(every) - len(bad_cost)}/{len(every)} calls")
        print()
        if honoured:
            print(f"VERDICT: Mistral honours {', '.join(honoured)} on chat completions.")
            print("A replayed call returns the original response and is not re-billed,")
            print("so the reclaim double-billing gap can be closed provider-side.")
            return 0

        print("VERDICT: Mistral does not honour an idempotency key on chat completions.")
        print("Both header spellings behaved exactly like the ignored nonsense header:")
        print("a second identical request regenerated, with a new id, and was billed again.")
        print()
        print("The README's stated fix — 'provider-side idempotency keys on egress' —")
        print("is therefore not available for this provider. Closing the gap needs a")
        print("mechanism that does not depend on provider cooperation.")
        return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
