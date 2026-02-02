#!/usr/bin/env python3
import json
import sys


def main():
    raw = sys.stdin.read()
    if not raw.strip():
        print(json.dumps({"skip_default": False}))
        return

    req = json.loads(raw)
    har = req.get("har") or {}
    log = har.get("log") or {}
    entries = log.get("entries") or []

    sys.stderr.write(f"[sample-exporter] entries={len(entries)}\n")
    # Keep default export output
    print(json.dumps({"skip_default": False}))


if __name__ == "__main__":
    main()
