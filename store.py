"""Persistent storage for group context flow.

Records are kept in an append-only JSONL log per group flow. Appending is a
true O(1) tail append; the log is only rewritten wholesale when the record
count exceeds ``max_log_records`` by a margin, which amortizes the cost of
keeping disk usage bounded.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


class GroupFlowStore:
    TRIM_MARGIN = 500

    def __init__(self, base_dir: Path, max_log_records: int = 5000) -> None:
        self.base_dir = base_dir
        self.logs_dir = base_dir / "logs"
        self.state_path = base_dir / "state.json"
        self.max_log_records = max(0, int(max_log_records or 0))
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Per-flow in-memory caches so appends never re-read the whole log.
        self._count_cache: dict[str, int] = {}
        self._seq_cache: dict[str, int] = {}
        # message_id -> seq, bounded to the recent window for dedup
        self._id_cache: dict[str, dict[str, int]] = {}
        self._id_cache_limit = 500

    def _flow_hash(self, flow_id: str) -> str:
        return hashlib.sha256(flow_id.encode("utf-8")).hexdigest()[:32]

    def _log_path(self, flow_id: str) -> Path:
        return self.logs_dir / f"{self._flow_hash(flow_id)}.jsonl"

    def _cursor_key(self, flow_id: str, conversation_id: str) -> str:
        return f"{self._flow_hash(flow_id)}:{conversation_id}"

    def _read_json_file(self, path: Path, default: Any) -> Any:
        if not path.is_file():
            return default
        try:
            with path.open("r", encoding="utf-8") as file:
                return json.load(file)
        except (OSError, json.JSONDecodeError):
            return default

    def _write_json_file(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp_path, path)

    def _load_flow(self, flow_id: str) -> None:
        """Load seq/count/recent-id caches for a flow once per process."""
        path = self._log_path(flow_id)
        count = 0
        last_seq = 0
        id_map: dict[str, int] = {}
        if path.is_file():
            with path.open("r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if not line:
                        continue
                    count += 1
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(item, dict):
                        continue
                    seq = int(item.get("seq") or 0)
                    if seq > last_seq:
                        last_seq = seq
                    message_id = str(item.get("message_id") or "")
                    if message_id:
                        id_map[message_id] = seq
        self._count_cache[flow_id] = count
        self._seq_cache[flow_id] = last_seq
        self._id_cache[flow_id] = dict(list(id_map.items())[-self._id_cache_limit:])

    def read_records(self, flow_id: str) -> list[dict[str, Any]]:
        path = self._log_path(flow_id)
        if not path.is_file():
            return []
        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    records.append(item)
        return records

    def append_record(self, flow_id: str, record: dict[str, Any]) -> int:
        if flow_id not in self._seq_cache:
            self._load_flow(flow_id)

        message_id = str(record.get("message_id") or "")
        if message_id and message_id in self._id_cache.get(flow_id, {}):
            return int(self._id_cache[flow_id][message_id])

        seq = int(self._seq_cache.get(flow_id) or 0) + 1
        record["seq"] = seq

        path = self._log_path(flow_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            file.write("\n")

        self._seq_cache[flow_id] = seq
        self._count_cache[flow_id] = int(self._count_cache.get(flow_id) or 0) + 1
        if message_id:
            id_map = self._id_cache.setdefault(flow_id, {})
            id_map[message_id] = seq
            if len(id_map) > self._id_cache_limit:
                self._id_cache[flow_id] = dict(
                    list(id_map.items())[-self._id_cache_limit:]
                )

        if (
            self.max_log_records > 0
            and int(self._count_cache.get(flow_id) or 0)
            > self.max_log_records + self.TRIM_MARGIN
        ):
            self._trim(flow_id)
        return seq

    def _trim(self, flow_id: str) -> None:
        """Rewrite the log keeping only the tail up to ``max_log_records``."""
        path = self._log_path(flow_id)
        records = self.read_records(flow_id)
        if len(records) <= self.max_log_records:
            self._count_cache[flow_id] = len(records)
            return
        records = records[-self.max_log_records :]
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as file:
            for record in records:
                file.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
                file.write("\n")
        os.replace(tmp_path, path)
        self._count_cache[flow_id] = len(records)
        id_map = {
            str(r.get("message_id") or ""): int(r.get("seq") or 0)
            for r in records
            if r.get("message_id")
        }
        self._id_cache[flow_id] = dict(list(id_map.items())[-self._id_cache_limit:])

    def find_seq_by_message_id(self, flow_id: str, message_id: str) -> int | None:
        if not message_id:
            return None
        for record in self.read_records(flow_id):
            if str(record.get("message_id") or "") == message_id:
                return int(record.get("seq") or 0)
        return None

    def get_range(self, flow_id: str, start_seq: int, end_seq: int) -> list[dict[str, Any]]:
        if end_seq < start_seq:
            return []
        return [
            record
            for record in self.read_records(flow_id)
            if start_seq <= int(record.get("seq") or 0) <= end_seq
        ]

    def read_state(self) -> dict[str, Any]:
        state = self._read_json_file(self.state_path, {})
        return state if isinstance(state, dict) else {}

    def write_state(self, state: dict[str, Any]) -> None:
        self._write_json_file(self.state_path, state)

    def get_cursor(self, flow_id: str, conversation_id: str) -> int:
        state = self.read_state()
        cursors = state.get("cursors", {})
        if not isinstance(cursors, dict):
            return 0
        value = cursors.get(self._cursor_key(flow_id, conversation_id), 0)
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def set_cursor(
        self,
        flow_id: str,
        conversation_id: str,
        seq: int,
        *,
        unified_msg_origin: str,
    ) -> None:
        state = self.read_state()
        cursors = state.setdefault("cursors", {})
        cursor_meta = state.setdefault("cursor_meta", {})
        if not isinstance(cursors, dict):
            cursors = {}
            state["cursors"] = cursors
        if not isinstance(cursor_meta, dict):
            cursor_meta = {}
            state["cursor_meta"] = cursor_meta

        key = self._cursor_key(flow_id, conversation_id)
        cursors[key] = max(0, int(seq or 0))
        cursor_meta[key] = {
            "flow_id": flow_id,
            "conversation_id": conversation_id,
            "unified_msg_origin": unified_msg_origin,
        }
        self.write_state(state)

    def clear_flow(self, flow_id: str) -> None:
        path = self._log_path(flow_id)
        if path.exists():
            path.unlink()

        flow_hash = self._flow_hash(flow_id)
        state = self.read_state()
        for section_name in ("cursors", "cursor_meta"):
            section = state.get(section_name, {})
            if isinstance(section, dict):
                for key in list(section.keys()):
                    if str(key).startswith(f"{flow_hash}:"):
                        section.pop(key, None)
        self.write_state(state)
        self._count_cache.pop(flow_id, None)
        self._seq_cache.pop(flow_id, None)
        self._id_cache.pop(flow_id, None)

    def stats(self, flow_id: str, conversation_id: str | None = None) -> dict[str, int]:
        records = self.read_records(flow_id)
        latest_seq = max((int(item.get("seq") or 0) for item in records), default=0)
        cursor = self.get_cursor(flow_id, conversation_id) if conversation_id else 0
        return {
            "records": len(records),
            "latest_seq": latest_seq,
            "cursor": cursor,
        }
