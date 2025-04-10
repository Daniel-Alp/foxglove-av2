import os
import struct

from glob import glob

from pathlib import Path

from av2.utils.io import read_feather

from mcap_protobuf.writer import Writer
from foxglove_schemas_protobuf.Vector3_pb2 import Vector3
from foxglove_schemas_protobuf.Quaternion_pb2 import Quaternion
from foxglove_schemas_protobuf.Pose_pb2 import Pose
from foxglove_schemas_protobuf.PackedElementField_pb2 import PackedElementField
from foxglove_schemas_protobuf.PointCloud_pb2 import PointCloud

from timestamp import make_protobuf_timestamp, fpath_to_timestamp_ns
from args import get_args

def av2_lidar_to_mcap(dataroot: Path, log_id: Path):
    try:
        with open(f"{log_id}-lidar.mcap", "wb") as stream, Writer(stream) as writer:
            if not Path.exists(dataroot / log_id / "sensors/lidar/"):
                e = FileNotFoundError()
                e.filename =f"{dataroot}/{log_id}/sensors/lidar/"
                raise e
            
            fpaths = sorted(glob(f"{dataroot}/{log_id}/sensors/lidar/*.feather"))
            for fpath in fpaths:
                data = read_feather(Path(fpath))

                timestamp_ns = fpath_to_timestamp_ns(fpath)
                timestamp = make_protobuf_timestamp(timestamp_ns)

                rows = len(data)
                pt_stride = 12
                buffer = bytearray(rows * pt_stride)
                offset = 0
                for pt in data.loc[:].to_numpy():
                    x, y, z, _, _, _ = pt
                    struct.pack_into("<fff", buffer, offset, x, y, z)
                    offset += pt_stride
                
                fields = [
                    PackedElementField(name="x", offset=0, type=7),
                    PackedElementField(name="y", offset=4, type=7),
                    PackedElementField(name="z", offset=8, type=7)
                ]
                
                pointcloud_msg = PointCloud(
                    timestamp    = timestamp,
                    frame_id     = "base_link",
                    pose         = Pose(position=Vector3(x=0, y=0, z=0), orientation=Quaternion(x=0, y=0, z=0, w=0)),
                    point_stride = pt_stride,
                    fields       = fields,
                    data         = bytes(buffer)
                )

                writer.write_message(
                    topic        = "/LIDAR_TOP",
                    message      = pointcloud_msg,
                    log_time     = timestamp_ns,
                    publish_time = timestamp_ns
                )
    except FileNotFoundError as e:
        print(f"Failed to convert lidar data to MCAP. File not found `{e.filename}`")
        os.remove(f"{log_id}-lidar.mcap")

if __name__ == "__main__":
    dataroot, log_id = get_args()
    av2_lidar_to_mcap(dataroot, log_id)