"""Byte-accurate streaming CSV parser.

Design
------
CSV structural bytes (comma, double quote, CR, LF) are single ASCII bytes.
In UTF-8 they never occur inside a multi-byte sequence; in GB18030 they never
occur as a trail byte, so an RFC 4180 style state machine can run on raw
bytes and fields can be decoded afterwards.

The parser consumes fixed-size chunks (down to a single byte). It keeps one
contiguous buffer holding the current, possibly partial record; completed
record prefixes are trimmed after every chunk, so a whole file is never held
in memory. All positions are absolute file offsets with BOM excluded. Record
spans are half-open [start, end) and include the terminating CR/LF when
present.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import ParseError

COMMA = 0x2C
QUOTE = 0x22
CR = 0x0D
LF = 0x0A
BOM = b"\xef\xbb\xbf"

_FIELD_START = 0
_UNQUOTED = 1
_QUOTED = 2
_QUOTE_PENDING = 3
_CR_AFTER_UNQUOTED = 4
_CR_AFTER_QUOTE = 5


@dataclass(frozen=True)
class Record:
    """One fully parsed CSV record."""

    start_offset: int
    end_offset: int
    raw: bytes
    fields: tuple[str, ...]
    line_index: int  # zero-based record index including the header


class StreamingCSVParser:
    """Incremental RFC-4180-style parser over a chunked byte stream."""

    def __init__(self, encoding: str, expected_columns: int):
        if encoding not in ("utf-8-sig", "utf-8", "gb18030"):
            raise ParseError(f"unsupported encoding: {encoding}")
        self._encoding = "utf-8" if encoding.startswith("utf-8") else encoding
        self._bom_expected = encoding == "utf-8-sig"
        self._expected_columns = expected_columns
        self._reset_stream()

    def _reset_stream(self) -> None:
        self._state = _FIELD_START
        self._buf = bytearray()
        self._record_start = 0  # absolute offset of _buf[0], BOM excluded
        self._consumed = 0  # absolute non-BOM bytes consumed
        self._field_spans: list[tuple[int, int, bool]] = []
        self._field_start = 0  # absolute content start of current field
        self._quote_close = None  # absolute offset of closing quote
        self._line_index = 0
        self._bom_buf = bytearray()
        self._bom_done = not self._bom_expected
        self._finished = False

    def feed(self, chunk: bytes) -> list[Record]:
        if self._finished:
            raise ParseError("feed() called after finish()", self._consumed)
        if not chunk:
            return []
        if not self._bom_done:
            chunk = self._consume_bom(chunk)
            if not chunk:
                return []
        self._consumed += len(chunk)
        self._buf.extend(chunk)
        out: list[Record] = []
        n = len(self._buf)
        scan = n - len(chunk)  # only scan bytes newly appended in this feed
        state = self._state

        while scan < n:
            if scan < 0:  # completed records trimmed earlier in this feed
                scan = 0
            position = self._record_start + scan
            byte = self._buf[scan]

            if state == _FIELD_START:
                self._field_start = position
                if byte == QUOTE:
                    state = _QUOTED
                    scan += 1
                    continue
                elif byte == COMMA:
                    self._emit_field(position, False)
                    scan += 1
                    continue
                elif byte == CR:
                    self._emit_field(position, False)
                    state = _CR_AFTER_UNQUOTED
                    scan += 1
                    continue
                elif byte == LF:
                    self._emit_field(position, False)
                    cut = self._finish_record(position + 1, out)
                    scan -= cut
                    n -= cut
                    state = _FIELD_START
                    scan += 1
                    continue
                else:
                    state = _UNQUOTED
                    # Fall through to _UNQUOTED with the same byte.

            if state == _UNQUOTED:
                if byte == COMMA:
                    self._emit_field(position, False)
                    state = _FIELD_START
                elif byte == CR:
                    self._emit_field(position, False)
                    state = _CR_AFTER_UNQUOTED
                elif byte == LF:
                    self._emit_field(position, False)
                    cut = self._finish_record(position + 1, out)
                    scan -= cut
                    n -= cut
                    state = _FIELD_START
                elif byte == QUOTE:
                    raise ParseError(
                        "bare double quote is not allowed in an unquoted field",
                        position,
                    )
                scan += 1
                continue

            if state == _QUOTED:
                if byte == QUOTE:
                    self._quote_close = position
                    state = _QUOTE_PENDING
                scan += 1
                continue

            if state == _QUOTE_PENDING:
                if byte == QUOTE:  # escaped quote ""
                    self._quote_close = None
                    state = _QUOTED
                elif byte == COMMA:
                    self._emit_field(position, True)
                    state = _FIELD_START
                elif byte == CR:
                    self._emit_field(position, True)
                    state = _CR_AFTER_QUOTE
                elif byte == LF:
                    self._emit_field(position, True)
                    cut = self._finish_record(position + 1, out)
                    scan -= cut
                    n -= cut
                    state = _FIELD_START
                else:
                    raise ParseError(
                        "characters after closing quote must be comma, "
                        "CR or LF",
                        position,
                    )
                scan += 1
                continue

            if state in (_CR_AFTER_UNQUOTED, _CR_AFTER_QUOTE):
                if byte == LF:
                    # Field already emitted at the CR; span includes the LF.
                    cut = self._finish_record(position + 1, out)
                    scan -= cut
                    n -= cut
                    state = _FIELD_START
                    scan += 1
                else:
                    # Bare CR terminated the record; rescan this byte.
                    cut = self._finish_record(position, out)
                    scan -= cut
                    n -= cut
                    state = _FIELD_START
                continue

            raise ParseError(f"unknown parser state {state}", position)  # pragma: no cover

        self._state = state
        return out

    def finish(self) -> list[Record]:
        if self._finished:
            return []
        self._finished = True
        if not self._bom_done and bytes(self._bom_buf):
            raise ParseError("file ends with an incomplete UTF-8 BOM", 0)
        out: list[Record] = []
        state = self._state
        end_position = self._record_start + len(self._buf)
        if state == _QUOTED:
            raise ParseError(
                "unclosed quoted field at end of file", end_position
            )
        if state == _QUOTE_PENDING:
            if self._quote_close is None:  # pragma: no cover - defensive
                raise ParseError("quoted field without closing quote")
            self._emit_field(self._quote_close, True)
            self._finish_record(end_position, out)
        elif state in (_CR_AFTER_UNQUOTED, _CR_AFTER_QUOTE):
            self._finish_record(end_position, out)
        elif state == _UNQUOTED:
            self._emit_field(end_position, False)
            self._finish_record(end_position, out)
        elif state == _FIELD_START and self._buf:
            self._emit_field(end_position, False)
            self._finish_record(end_position, out)
        elif self._buf:  # pragma: no cover - defensive
            raise ParseError("dangling parser buffer at end of file")
        return out

    def _consume_bom(self, chunk: bytes) -> bytes:
        self._bom_buf.extend(chunk)
        prefix = bytes(self._bom_buf)
        if prefix.startswith(BOM):
            self._bom_done = True
            remainder = prefix[len(BOM):]
            self._bom_buf.clear()
            return remainder
        if BOM.startswith(prefix) and len(prefix) <= len(BOM):
            return b""  # BOM split across chunks; wait for more bytes
        raise ParseError(
            "declared utf-8-sig file does not start with BOM", 0
        )

    def _emit_field(self, end_position: int, quoted: bool) -> None:
        """Record a field with half-open absolute [start, end) content span."""
        start = self._field_start
        if quoted:
            if self._quote_close is None:  # pragma: no cover - defensive
                raise ParseError("quoted field missing closing quote")
            start += 1  # skip opening quote
            end = self._quote_close
        else:
            end = end_position
        self._field_spans.append((start, end, quoted))
        self._quote_close = None

    def _finish_record(self, end_position: int, out: list[Record]) -> int:
        end_scan = end_position - self._record_start
        raw = bytes(self._buf[:end_scan])
        fields = self._decode_fields(raw)
        self._field_spans.clear()
        self._quote_close = None
        if len(fields) != self._expected_columns:
            raise ParseError(
                f"column count mismatch: expected {self._expected_columns}, "
                f"got {len(fields)}",
                self._record_start,
            )
        out.append(
            Record(
                start_offset=self._record_start,
                end_offset=end_position,
                raw=raw,
                fields=fields,
                line_index=self._line_index,
            )
        )
        self._line_index += 1
        # Trim immediately so absolute positions stay valid even when many
        # records arrive inside a single large chunk.
        del self._buf[:end_scan]
        self._record_start = end_position
        return end_scan

    def _decode_fields(self, raw: bytes) -> tuple[str, ...]:
        decoded: list[str] = []
        for start, end, quoted in self._field_spans:
            rel_start = start - self._record_start
            rel_end = end - self._record_start
            field_bytes = raw[rel_start:rel_end]
            if quoted:
                field_bytes = field_bytes.replace(b'""', b'"')
            try:
                decoded.append(field_bytes.decode(self._encoding))
            except UnicodeDecodeError as exc:
                raise ParseError(
                    f"illegal {self._encoding} byte sequence in field: {exc}",
                    start,
                ) from exc
        return tuple(decoded)


def stream_records(
    binary_stream,
    encoding: str,
    expected_columns: int,
    block_size: int,
):
    """Yield :class:`Record` objects from a binary stream.

    Reads at most ``block_size`` bytes at a time and never holds more than
    one record plus one chunk in memory.
    """
    if block_size < 1:
        raise ValueError("block_size must be >= 1")
    parser = StreamingCSVParser(encoding, expected_columns)
    while True:
        chunk = binary_stream.read(block_size)
        if not chunk:
            break
        yield from parser.feed(chunk)
    yield from parser.finish()
