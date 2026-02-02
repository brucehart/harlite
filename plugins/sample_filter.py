#!/usr/bin/env python3
import json
import sys


def main():
    raw = sys.stdin.read()
    if not raw.strip():
        print(json.dumps({"allow": True}))
        return

    req = json.loads(raw)
    entry = req.get("entry") or {}
    request = entry.get("request") or {}
    url = request.get("url", "")

    # Example: drop analytics/beacon calls
    blocked = "analytics" in url or "/beacon" in url
    print(json.dumps({"allow": not blocked}))


if __name__ == "__main__":
    main()
