from common.message_protocol.internal import InternalMessage
import uuid

class MessageHandler:
    def __init__(self):
        self._client_id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        fruit, amount = message
        internal_msg = InternalMessage(client_id=self._client_id, data=[fruit, amount])
        return internal_msg.serialize()


    def serialize_eof_message(self, _message=None):
        internal_msg = InternalMessage(client_id=self._client_id, data=None)
        return internal_msg.serialize()

    def deserialize_result_message(self, message):
        msg = InternalMessage.deserialize(message)
        
        if msg.client_id == self._client_id:
            return msg.data
        return None