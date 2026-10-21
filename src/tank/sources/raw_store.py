"""Immutable raw-item files using the literal HLD §7.2 layout."""

import os
import re
from pathlib import Path

import zstandard

from tank.util import new_id, parse_time, sha256


def raw_item_path(data_dir, source_id, item_id, observed_at):
    for name, value in (("source_id", source_id), ("item_id", item_id)):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
            raise ValueError(f"Invalid raw-item {name}")
    observed = parse_time(observed_at)
    return (
        Path(data_dir).resolve()
        / "raw"
        / "items"
        / source_id
        / observed.strftime("%Y/%m/%d")
        / (item_id + ".json.zst")
    )


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sync_hierarchy(directory, data_dir):
    boundary = Path(data_dir).resolve().parent
    while True:
        _sync_directory(directory)
        if directory == boundary:
            break
        directory = directory.parent


def write_raw_item(data_dir, source_id, item_id, observed_at, payload):
    """Return only after exact bytes and their directory entries are durable.

    An exception leaves no permission to persist the returned reference. A file
    may already have been published when directory fsync fails; an identical
    retry validates and flushes it again before reporting success.
    """
    if not isinstance(payload, bytes):
        raise TypeError("Raw payload must be bytes")
    path = raw_item_path(data_dir, source_id, item_id, observed_at)
    digest = sha256(payload)
    if path.exists():
        with path.open("rb") as stored_file:
            stored = zstandard.ZstdDecompressor().decompress(stored_file.read())
            if stored != payload:
                raise ValueError("Conflicting immutable raw-item payload")
            os.fsync(stored_file.fileno())
        _sync_hierarchy(path.parent, data_dir)
        return str(path), digest
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix("." + new_id("raw") + ".tmp")
    try:
        with temporary.open("xb") as compressed_file:
            compressed_file.write(zstandard.ZstdCompressor(level=3).compress(payload))
            compressed_file.flush()
            os.fsync(compressed_file.fileno())
        try:
            path.hardlink_to(temporary)
        except FileExistsError:
            with path.open("rb") as stored_file:
                if zstandard.ZstdDecompressor().decompress(stored_file.read()) != payload:
                    raise ValueError("Conflicting immutable raw-item payload") from None
                os.fsync(stored_file.fileno())
        temporary.unlink()
        _sync_hierarchy(path.parent, data_dir)
    finally:
        if temporary.exists():
            temporary.unlink()
            _sync_directory(path.parent)
    return str(path), digest
