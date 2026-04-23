import os
import logging

from common import middleware, fruit_item
from common.message_protocol.internal import InternalMessage

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.data_per_client = {}
        self.eof_count_per_client = {}

    def _process_data(self, client_id, data):
        if client_id not in self.data_per_client:
            self.data_per_client[client_id] = []
        client_inventory = self.data_per_client[client_id]
        for fruit, amount in data:
            client_inventory.append(fruit_item.FruitItem(fruit, int(amount)))

    def _process_eof(self, client_id):
        self.eof_count_per_client[client_id] = self.eof_count_per_client.get(client_id, 0) + 1
        if self.eof_count_per_client[client_id] < AGGREGATION_AMOUNT:
            return

        all_items = sorted(self.data_per_client.get(client_id, []))
        top_items = all_items[-TOP_SIZE:]
        top_items.reverse()
        fruit_top_data = [(item.fruit, item.amount) for item in top_items]

        self.output_queue.send(InternalMessage(client_id=client_id, data=fruit_top_data).serialize())

        if client_id in self.data_per_client:
            del self.data_per_client[client_id]
        del self.eof_count_per_client[client_id]
    
    def process_message(self, message, ack, nack):
        internal_message = InternalMessage.deserialize(message)
        client_id = internal_message.client_id
        if internal_message.data:
            self._process_data(client_id, internal_message.data)
        else:
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
