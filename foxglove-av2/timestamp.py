from pathlib import Path

from google.protobuf.timestamp_pb2 import Timestamp

def make_timestamp(timestamp_ns: int) -> Timestamp:
    seconds = timestamp_ns // 1_000_000_000
    nanos = timestamp_ns % 1_000_000_000
    return Timestamp(seconds=seconds, nanos=nanos)

def get_timestamp_ns(fpath: str) -> int:
    return int(Path(fpath).stem.split(".")[0])