"""Count completed S3 requests from an unbuffered, local proxy access log."""

import json
from collections import Counter
from urllib.parse import parse_qs


def request_class(record):
    method = record["method"]
    query = parse_qs(record["args"], keep_blank_values=True)
    if method == "GET" and any(k in query for k in ("list-type", "prefix", "delimiter", "uploads")):
        return "LIST"
    if method == "POST" and "delete" in query:
        return "DELETE"
    return method


class RequestCounter:
    def __init__(self, path):
        self.path = path
        self.offset = 0
        self.counts = Counter()

    def read(self):
        with self.path.open("rb") as stream:
            if self.path.stat().st_size < self.offset:
                raise ValueError("S3 access log was truncated during measurement")
            stream.seek(self.offset)
            while line := stream.readline():
                if not line.endswith(b"\n"):
                    break  # Retry a concurrently written partial line on the next read.
                record = json.loads(line)
                self.counts[request_class(record)] += 1
                if record["status"] >= 400:
                    self.counts[f"HTTP_{record['status']}"] += 1
                self.offset = stream.tell()
        return dict(self.counts)
