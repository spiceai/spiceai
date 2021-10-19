import json
from proto.aiengine.v1 import aiengine_pb2


def get_init_from_json(init_data_path: str, pod_name: str) -> aiengine_pb2.InitRequest:
    with open(init_data_path, "r", encoding="utf-8") as pod_init:
        pod_init_bytes = pod_init.read()
    pod_init_json = json.loads(pod_init_bytes)

    pod_fields = {}
    for field in pod_init_json["fields"]:
        initializer = (
            float(0)
            if "initializer" not in pod_init_json["fields"][field]
            else float(pod_init_json["fields"][field]["initializer"])
        )
        fill_method = (
            aiengine_pb2.FILL_FORWARD
            if "fill_method" not in pod_init_json["fields"][field]
            else int(pod_init_json["fields"][field]["fill_method"])
        )
        field_data = aiengine_pb2.FieldData(
            initializer=initializer, fill_method=fill_method
        )
        pod_fields[field] = field_data
    pod_init_req = aiengine_pb2.InitRequest(
        pod=pod_name,
        period=int(pod_init_json["period"]),
        interval=int(pod_init_json["interval"]),
        granularity=int(pod_init_json["granularity"]),
        epoch_time=int(pod_init_json["epoch_time"]),
        fields=pod_fields,
        actions=pod_init_json["actions"],
        actions_order=pod_init_json["actions_order"],
        laws=pod_init_json["laws"],
    )
    for datasource in pod_init_json["datasources"]:
        ai_datasource = aiengine_pb2.DataSource(
            connector=aiengine_pb2.DataConnector(name=datasource["connector"]["name"]),
            actions=datasource["actions"],
        )
        if "params" in datasource["connector"]:
            for key, val in datasource["connector"]["params"].items():
                ai_datasource.connector.params[key] = val
        pod_init_req.datasources.append(ai_datasource)

    return pod_init_req
