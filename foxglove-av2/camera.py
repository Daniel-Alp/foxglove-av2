from glob import glob

from pathlib import Path

from av2.utils.io import read_feather

from mcap_protobuf.writer import Writer
from foxglove_schemas_protobuf.CompressedImage_pb2 import CompressedImage
from foxglove_schemas_protobuf.CameraCalibration_pb2 import CameraCalibration
from foxglove_schemas_protobuf.Vector3_pb2 import Vector3
from foxglove_schemas_protobuf.Quaternion_pb2 import Quaternion
from foxglove_schemas_protobuf.FrameTransform_pb2 import FrameTransform

from timestamp import make_protobuf_timestamp, fpath_to_timestamp_ns

def av2_camera_to_mcap(dataroot: Path, log_id: Path):
    with open(f"{log_id}-camera.mcap", "wb") as stream, Writer(stream) as writer:
        calibs = read_feather(dataroot / log_id / "calibration/egovehicle_SE3_sensor.feather")
        intrins = read_feather(dataroot / log_id / "calibration/intrinsics.feather")


        for cam_calib, cam_intrin in zip(calibs.to_numpy(), intrins.to_numpy()):
            id, qw, qx, qy, qz, x, y, z = cam_calib
            _, fx_px, fy_px, cx_px, cy_px, k1, k2, k3, height_px, width_px = cam_intrin

            img_fpaths = sorted(glob(f"{dataroot}/{log_id}/sensors/cameras/{id}/*"))

            # used as timestamp for camera calibration
            first_timestamp_ns = fpath_to_timestamp_ns(img_fpaths[0])
            timestamp = make_protobuf_timestamp(first_timestamp_ns)

            calib_msg = CameraCalibration(
                timestamp        = timestamp,
                frame_id         = id,
                width            = width_px,
                height           = height_px,
                distortion_model = "plumb_bob",
                D = [k1, k2,  0, 0, k3],
                K = [fx_px, 0,     cx_px, 
                     0,     fy_px, cy_px,
                     0,     0,     1    ],
                R = [1, 0, 0,
                     0, 1, 0,
                     0, 0, 1],
                P = [fx_px, 0,     cx_px, 0,
                     0,     fy_px, cy_px, 0,
                     0,     0,     1,     0],
            )
            writer.write_message(
                topic        = f"/camera/{id}/camera_info",
                message      = calib_msg,
                log_time     = first_timestamp_ns,
                publish_time = first_timestamp_ns
            )

            tf_msg = FrameTransform(
                timestamp       = timestamp,
                parent_frame_id = "base_link",
                child_frame_id  = id,
                translation     = Vector3(x=x, y=y, z=z),
                rotation        = Quaternion(x=qx, y=qy, z=qz, w=qw)
            )
            writer.write_message(
                topic        = "tf",
                message      = tf_msg,
                log_time     = first_timestamp_ns,
                publish_time = first_timestamp_ns
            )

            count = 0
            downsample_rate = 10

            for img_fpath in img_fpaths:
                count += 1
                if count % downsample_rate != 0:
                    continue
                timestamp_ns = fpath_to_timestamp_ns(img_fpath)
                timestamp = make_protobuf_timestamp(timestamp_ns)

                img = open(img_fpath, "rb")
                data = bytes(img.read())

                img_msg = CompressedImage(
                    timestamp = timestamp,
                    frame_id  = id,
                    data      =  data,
                    format    = "jpeg"
                )
                writer.write_message(
                    topic        = f"/camera/{id}/compressed_image_downsampled",
                    message      = img_msg,
                    log_time     = timestamp_ns,
                    publish_time = timestamp_ns
                )

if __name__ == "__main__":
    dataroot = Path("/home/alp/data/datasets")
    log_id = Path("00a6ffc1-6ce9-3bc3-a060-6006e9893a1a")
    av2_camera_to_mcap(dataroot, log_id)