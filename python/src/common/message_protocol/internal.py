from dataclasses import dataclass, asdict
import json

@dataclass
class InternalMessage:
    client_id: str
    data: list

    def serialize(self):
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def deserialize(cls, raw_bytes):
        data_dict = json.loads(raw_bytes.decode("utf-8"))
        return cls(**data_dict)