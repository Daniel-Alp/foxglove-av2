from glob import glob

from pathlib import Path

from mcap_protobuf.writer import Writer
from foxglove_schemas_protobuf.CompressedImage_pb2 import CompressedImage

from timestamp import make_protobuf_timestamp, get_timestamp_ns

def av2_camera_to_mcap(dataroot: Path, log_id: Path):
    with open(f"{log_id}-camera.mcap", "wb") as stream, Writer(stream) as writer:
        camera_fpaths = sorted(glob(f"{dataroot}/{log_id}/sensors/cameras/*"))

        for camera_fpath in camera_fpaths:
            topic = Path(camera_fpath).stem
            img_fpaths = sorted(glob(f"{camera_fpath}/*"))

            for img_fpath in img_fpaths:
                timestamp_ns = get_timestamp_ns(img_fpath)
                timestamp = make_protobuf_timestamp(timestamp_ns)

                img = open(img_fpath, "rb")
                data = bytes(img.read())

                img_msg = CompressedImage(
                    timestamp = timestamp,
                    frame_id  = topic,
                    data      =  data,
                    format    = "jpeg"
                )
                writer.write_message(
                    topic        = topic,
                    message      = img_msg,
                    log_time     = timestamp_ns,
                    publish_time = timestamp_ns
                )

if __name__ == "__main__":
    dataroot = Path("/home/alp/data/datasets")
    log_id = Path("00a6ffc1-6ce9-3bc3-a060-6006e9893a1a")
    av2_camera_to_mcap(dataroot, log_id)