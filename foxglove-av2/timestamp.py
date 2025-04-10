from pathlib import Path

from google.protobuf.timestamp_pb2 import Timestamp

def make_protobuf_timestamp(timestamp_ns: int) -> Timestamp:
    seconds = timestamp_ns // 1_000_000_000
    nanos = timestamp_ns % 1_000_000_000
    return Timestamp(seconds=seconds, nanos=nanos)

def fpath_to_timestamp_ns(fpath: str) -> int:
    return int(Path(fpath).stem.split(".")[0])

def protobuf_timestamp_to_timestamp_ns(timestamp: Timestamp) -> int:
    return timestamp.seconds * 1_000_000_000 + timestamp.nanos