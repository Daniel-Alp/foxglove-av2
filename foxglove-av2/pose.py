import os

from pathlib import Path

from av2.utils.io import read_feather

from mcap_protobuf.writer import Writer
from foxglove_schemas_protobuf.Vector3_pb2 import Vector3
from foxglove_schemas_protobuf.Quaternion_pb2 import Quaternion
from foxglove_schemas_protobuf.FrameTransform_pb2 import FrameTransform

from timestamp import make_protobuf_timestamp
from args import get_args

def av2_pose_to_mcap(dataroot: Path, log_id: Path):
    try:
        with open(f"{log_id}-pose.mcap", "wb") as stream, Writer(stream) as writer:
            data = read_feather(dataroot / log_id / "city_SE3_egovehicle.feather")
            for pose in data.loc[:].to_numpy():
                timestamp_ns, qw, qx, qy, qz, x, y, z = pose
                timestamp_ns = int(timestamp_ns)

                timestamp = make_protobuf_timestamp(timestamp_ns)

                tf_msg = FrameTransform(
                    timestamp       = timestamp,
                    parent_frame_id = "map",
                    child_frame_id  = "base_link",
                    translation     = Vector3(x=x, y=y, z=z),
                    rotation        = Quaternion(x=qx, y=qy, z=qz, w=qw)
                )

                writer.write_message(
                    topic        = "tf",
                    message      = tf_msg,
                    log_time     = timestamp_ns,
                    publish_time = timestamp_ns
                )
    except FileNotFoundError as e:
        print(f"Failed to convert pose data to MCAP. File not found `{e.filename}`")
        os.remove(f"{log_id}-pose.mcap")

if __name__ == "__main__":
    dataroot, log_id = get_args()
    av2_pose_to_mcap(dataroot, log_id)