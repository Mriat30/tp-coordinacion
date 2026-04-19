import os
import logging

from common import middleware, fruit_item
from common.message_protocol.internal import InternalMessage

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.data_per_client = {}
        self.eof_count_per_client = {}


    def process_message(self, message, ack, nack):
        logging.info("Process message")
        internal_message = InternalMessage.deserialize(message)
        client_id = internal_message.client_id
        data = internal_message.data
        if data:
            self._process_data(client_id, *data)
        else:
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data message for client {client_id}")
        if client_id not in self.data_per_client:
            self.data_per_client[client_id] = {}
        inventory_of_client = self.data_per_client[client_id]
        current_fruit_item = inventory_of_client.get(fruit,
                                                      fruit_item.FruitItem(fruit, 0))
        inventory_of_client[fruit] = current_fruit_item + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_id):
        self.eof_count_per_client[client_id] = self.eof_count_per_client.get(client_id, 0) + 1

        if self.eof_count_per_client[client_id] < SUM_AMOUNT:
            logging.info(f"EOF {self.eof_count_per_client[client_id]}/{SUM_AMOUNT} for client {client_id}, waiting for more")
            return

        # Tengo todos los EOFs, flusheo solo si tengo datos
        logging.info(f"All EOFs received for client {client_id}")
        if client_id in self.data_per_client:
            all_sorted_items = sorted(self.data_per_client[client_id].values())
            top_fruit_items = all_sorted_items[-TOP_SIZE:]
            top_fruit_items.reverse()
            fruit_top_data = [(item.fruit, item.amount) for item in top_fruit_items]
            self.output_queue.send(InternalMessage(client_id=client_id, data=fruit_top_data).serialize())
            del self.data_per_client[client_id]

        
def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
