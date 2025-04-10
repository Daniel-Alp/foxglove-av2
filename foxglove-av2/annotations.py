from pathlib import Path

from av2.map.map_api import ArgoverseStaticMap
from av2.utils.io import read_feather

from mcap_protobuf.writer import Writer
from foxglove_schemas_protobuf.Point3_pb2 import Point3
from foxglove_schemas_protobuf.Vector3_pb2 import Vector3
from foxglove_schemas_protobuf.Quaternion_pb2 import Quaternion
from foxglove_schemas_protobuf.Pose_pb2 import Pose
from foxglove_schemas_protobuf.LinePrimitive_pb2 import LinePrimitive
from foxglove_schemas_protobuf.CubePrimitive_pb2 import CubePrimitive
from foxglove_schemas_protobuf.ModelPrimitive_pb2 import ModelPrimitive
from foxglove_schemas_protobuf.Color_pb2 import Color
from foxglove_schemas_protobuf.KeyValuePair_pb2 import KeyValuePair
from foxglove_schemas_protobuf.SceneEntity_pb2 import SceneEntity
from foxglove_schemas_protobuf.SceneEntityDeletion_pb2 import SceneEntityDeletion
from foxglove_schemas_protobuf.SceneUpdate_pb2 import SceneUpdate

from google.protobuf.duration_pb2 import Duration

from timestamp import make_protobuf_timestamp, protobuf_timestamp_to_timestamp_ns

def av2_annotations_to_mcap(dataroot: Path, log_id: Path):
    with open(f"{log_id}-annotations.mcap", "wb") as stream, Writer(stream) as writer:
        # first convert every annotation to a scene entity, then create annotation scene updates
        annotations = read_feather(dataroot / log_id / "annotations.feather")
        rows = len(annotations)
        # every entity for every point in time, sorted by timestamp_ns
        all_entities: list[SceneEntity] = [None] * rows

        for i, entity in enumerate(annotations.loc[:].to_numpy()):
            timestamp_ns, track_uuid, category, l, w, h, qw, qx, qy, qz, x, y, z, _ = entity

            cube = CubePrimitive(
                pose  = Pose(position=Vector3(x=x, y=y, z=z), orientation=Quaternion(x=qx, y=qy, z=qz, w=qw)),
                size  = Vector3(x=l, y=w, z=h),
                color = Color(r=1, g=0.65, b=0, a=0.6),
            )
            
            all_entities[i] = SceneEntity(
                timestamp    = make_protobuf_timestamp(timestamp_ns),
                frame_id     = "base_link",
                id           = track_uuid,
                lifetime     = Duration(seconds=0, nanos=0),
                frame_locked = True,
                metadata     = [KeyValuePair(key="category", value=category)],
                cubes        = [cube]
            )

        # timestamp of first annotations scene update
        sceneupdate_timestamp_ns = protobuf_timestamp_to_timestamp_ns(all_entities[0].timestamp)
        # save this value because it will be used when publishing map scene update
        first_sceneupdate_timestamp_ns = sceneupdate_timestamp_ns

        # entities for a particular scene update
        annotation_entities = []
        # uuids for previous and current scene update, used to determine entity deletions
        prev_uuids = []
        curr_uuids = []

        for entity in all_entities:
            entity_timestamp_ns = protobuf_timestamp_to_timestamp_ns(entity.timestamp)

            # current entity belongs to next scene update, now we can publish a scene update message
            if entity_timestamp_ns > sceneupdate_timestamp_ns:
                diff = list(set(prev_uuids) - set(curr_uuids))
                annotation_deletions = [SceneEntityDeletion(timestamp=entity.timestamp, type=0, id=uuid) for uuid in diff]

                sceneupdate_msg = SceneUpdate(
                    entities  = annotation_entities,
                    deletions = annotation_deletions
                )
                writer.write_message(
                    topic        = "/markers/annotations",
                    message      = sceneupdate_msg,
                    log_time     = sceneupdate_timestamp_ns,
                    publish_time = sceneupdate_timestamp_ns
                )

                sceneupdate_timestamp_ns = entity_timestamp_ns
                annotation_entities = []
                prev_uuids = curr_uuids
                curr_uuids = []

            annotation_entities.append(entity)
            curr_uuids.append(entity.id)

        map = ArgoverseStaticMap.from_map_dir(dataroot / log_id / "map")
        lane_segments = map.vector_lane_segments.items()

        map_entities: list[SceneEntity] = [None] * len(lane_segments)

        for id, (_, segment) in enumerate(lane_segments):
            pts = [Point3(x=x, y=y, z=z) for x, y, z in segment.polygon_boundary]
            line = LinePrimitive(
                type            = 0,
                pose            = Pose(position=Vector3(x=0, y=0, z=0), orientation=Quaternion(x=0, y=0, z=0, w=1)),
                thickness       = 0.1,
                scale_invariant = False,
                points          = pts,
                color           = Color(r=0.2, g=0.627, b=0.173, a=1)
            )
            map_entities[id] = SceneEntity(
                timestamp    = make_protobuf_timestamp(first_sceneupdate_timestamp_ns),
                frame_id     = "map",
                id           = str(id),
                lifetime     = Duration(seconds=0, nanos=0), 
                frame_locked = True, 
                lines        = [line],
            )

        map_sceneupdate_msg = SceneUpdate(
            entities  = map_entities,
            deletions = []
        )
        writer.write_message(
            topic        = "/semantic_map",
            message      = map_sceneupdate_msg,
            log_time     = first_sceneupdate_timestamp_ns,
            publish_time = first_sceneupdate_timestamp_ns
        )
        
        car_model = ModelPrimitive(
            pose  = Pose(position=Vector3(x=0, y=0, z=0), orientation=Quaternion(x=0, y=0, z=1, w=0)),
            scale = Vector3(x=1, y=1, z=1),   
            url   = "https://raw.githubusercontent.com/Daniel-Alp/foxglove-av2/refs/heads/master/mesh/lexus.gltf"
        )
        car_entity = SceneEntity(
            timestamp    = make_protobuf_timestamp(first_sceneupdate_timestamp_ns),
            frame_id     = "base_link",
            id           = "car",
            lifetime     = Duration(seconds=0, nanos=0),
            frame_locked = True, 
            models       = [car_model]
        )
        car_sceneupdate_msg = SceneUpdate(
            entities  = [car_entity],
            deletions = []
        )
        writer.write_message(
            topic        = "/markers/car",
            message      = car_sceneupdate_msg,
            log_time     = first_sceneupdate_timestamp_ns,
            publish_time = first_sceneupdate_timestamp_ns
        )
        
if __name__ == "__main__":
    dataroot = Path("/home/alp/data/datasets/")
    log_id = Path("00a6ffc1-6ce9-3bc3-a060-6006e9893a1a")
    av2_annotations_to_mcap(dataroot, log_id)