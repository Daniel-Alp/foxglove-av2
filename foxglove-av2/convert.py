from annotations import av2_annotations_to_mcap
from camera import av2_camera_to_mcap
from lidar import av2_lidar_to_mcap
from pose import av2_pose_to_mcap
from args import get_args

if __name__ == "__main__":
    dataroot, log_id = get_args()
    av2_annotations_to_mcap(dataroot, log_id)
    av2_camera_to_mcap(dataroot, log_id)
    av2_lidar_to_mcap(dataroot, log_id)
    av2_pose_to_mcap(dataroot, log_id)
