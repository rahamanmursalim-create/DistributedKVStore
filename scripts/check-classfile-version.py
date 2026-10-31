#!/usr/bin/env python3
"""Fail if generated Java classes do not match the Java 17 class-file version."""

from __future__ import annotations

from pathlib import Path
import struct
import sys

EXPECTED_MAJOR = 61


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: check-classfile-version.py <class-dir>...", file=sys.stderr)
        return 2
    files: list[Path] = []
    for argument in sys.argv[1:]:
        root = Path(argument)
        if not root.is_dir():
            print(f"Missing class directory: {root}", file=sys.stderr)
            return 2
        files.extend(root.rglob("*.class"))
    if not files:
        print("No generated .class files found.", file=sys.stderr)
        return 2
    failures = 0
    for path in files:
        try:
            with path.open("rb") as stream:
                header = stream.read(8)
        except OSError as error:
            print(f"Cannot read {path}: {error}", file=sys.stderr)
            failures += 1
            continue
        if len(header) != 8 or header[:4] != b"\xca\xfe\xba\xbe":
            print(f"Invalid or truncated class file: {path}", file=sys.stderr)
            failures += 1
            continue
        major = struct.unpack(">H", header[6:8])[0]
        if major != EXPECTED_MAJOR:
            print(f"Expected class version {EXPECTED_MAJOR}, found {major}: {path}", file=sys.stderr)
            failures += 1
    if failures:
        return 1
    print(f"Verified Java 17 class-file version 61 in {len(files)} generated classes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
