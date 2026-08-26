"""A minimal hand-rolled HTTP/1.1 chunked-streaming server.

Used only to get ground truth on whether a client's cancellation actually
closes the transport. Deliberately not aiohttp/uvicorn: a framework adds
buffering and abstraction layers between "we called write()" and "the
kernel actually put bytes on the wire," which could mask exactly the
signal this test needs. Raw asyncio.start_server keeps that path as short
and observable as possible.

This stands in for an expensive streaming LLM provider. It never knows or
cares about Axiom's fencing logic — it just writes chunks slowly and
records, honestly, when a write starts failing.
"""

import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class StreamServerLog:
    """Timestamped record of what the server actually observed — the ground truth.

    Everything here is measured server-side. Nothing here trusts the
    client's own account of what it did.
    """

    started_at: float = 0.0
    chunks_written: list[float] = field(default_factory=list)
    eof_observed_at: float | None = None
    write_failed_at: float | None = None
    closed_at: float | None = None

    def summary(self) -> str:
        """Render every observation relative to the stream's start, for test output."""
        n = len(self.chunks_written)
        last = self._rel(self.chunks_written[-1]) if n else None
        return (
            f"chunks_written={n}, last_chunk_at={last}, "
            f"eof_observed_at={self._rel(self.eof_observed_at)}, "
            f"write_failed_at={self._rel(self.write_failed_at)}, "
            f"closed_at={self._rel(self.closed_at)}"
        )

    def _rel(self, t: float | None) -> float | None:
        return None if t is None else round(t - self.started_at, 3)


class FakeStreamServer:
    """A local TCP server that streams chunks slowly and logs failed writes.

    A write failing with ConnectionResetError/BrokenPipeError is the
    ground-truth signal that the client's socket actually closed — not
    that our own code *believes* it closed the socket, but that the far
    end (this server) actually observed the TCP connection go away.
    """

    def __init__(self, *, chunk_interval_seconds: float = 0.2, total_chunks: int = 100):
        """Configure the stream's pacing and length; call start() to bind a port."""
        self.chunk_interval_seconds = chunk_interval_seconds
        self.total_chunks = total_chunks
        self.log = StreamServerLog()
        self._server: asyncio.base_events.Server | None = None
        self.port: int = 0

    async def start(self) -> None:
        """Bind an ephemeral loopback port and begin accepting one connection at a time."""
        self._server = await asyncio.start_server(self._handle, host="127.0.0.1", port=0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        """Close the listening socket and wait for it to release the port."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _watch_for_eof(self, reader: asyncio.StreamReader) -> None:
        """Record the instant the client's FIN arrives.

        This is the cadence-independent close signal. A failed write is
        structurally one write late: the client's FIN does not fail a
        server-to-client write (half-close still permits it) — that write
        provokes an RST, and only the *next* write fails. Reading instead
        observes the FIN itself, the moment it lands, with no dependence on
        how often this server happens to be writing.
        """
        try:
            if await reader.read(1) == b"":
                self.log.eof_observed_at = time.monotonic()
        except (ConnectionResetError, BrokenPipeError):
            self.log.eof_observed_at = time.monotonic()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Drain the request line/headers; this fake only ever serves one route.
        try:
            await reader.readuntil(b"\r\n\r\n")
        except asyncio.IncompleteReadError:
            return

        headers = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/event-stream\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Connection: close\r\n"
            "\r\n"
        )
        writer.write(headers.encode())
        await writer.drain()

        self.log.started_at = time.monotonic()
        eof_watcher = asyncio.create_task(self._watch_for_eof(reader))

        for i in range(self.total_chunks):
            body = f"chunk-{i}\n".encode()
            chunked = f"{len(body):x}\r\n".encode() + body + b"\r\n"
            try:
                writer.write(chunked)
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError):
                self.log.write_failed_at = time.monotonic()
                break
            self.log.chunks_written.append(time.monotonic())
            await asyncio.sleep(self.chunk_interval_seconds)
        else:
            # Only reached if we streamed every chunk without the client
            # ever disconnecting — the "cancellation never happened" case.
            try:
                writer.write(b"0\r\n\r\n")
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError):
                self.log.write_failed_at = time.monotonic()

        # Give the watcher a bounded chance to record an already-arrived FIN
        # before teardown; it never completes if the client stayed connected.
        try:
            await asyncio.wait_for(eof_watcher, timeout=0.5)
        except (TimeoutError, asyncio.CancelledError):
            eof_watcher.cancel()

        self.log.closed_at = time.monotonic()
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            pass