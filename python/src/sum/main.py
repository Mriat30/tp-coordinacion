import os
import logging
import threading

from common import middleware, fruit_item
from common.message_protocol.internal import InternalMessage

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.data_per_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data for client {client_id}")
        if client_id not in self.data_per_client:
            self.data_per_client[client_id] = {}
        total_fruits_of_client = self.data_per_client[client_id]

        # Aca basicamente lo que hacemos es decir, 
        # "si no existe la fruta(FruitItem) en el diccionario
        #  para este cliente lo inicializo con cantidad 0
        #  y lo devuelvo, si existe simplemente lo devuelvo"
        current_fruit_item = total_fruits_of_client.get(fruit, fruit_item.FruitItem(fruit, 0))
        total_fruits_of_client[fruit] = current_fruit_item + fruit_item.FruitItem(fruit,
                                                                                   int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Processing EOF for client {client_id}")
        for final_fruit_item in self.data_per_client[client_id].values():
            for data_output_exchange in self.data_output_exchanges:
                internal_msg = InternalMessage(client_id=client_id,
                                                data=[final_fruit_item.fruit,
                                                       final_fruit_item.amount])
                data_output_exchange.send(
                    internal_msg.serialize()
                )

        logging.info(f"Finished processing EOF for client {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(InternalMessage(client_id=client_id,
                                                       data=None).serialize())
        if client_id in self.data_per_client:
            del self.data_per_client[client_id]
        logging.info(f"Cleaned data for client {client_id}")


    def process_data_messsage(self, message, ack, nack):
        internal_message = InternalMessage.deserialize(message)
        client_id = internal_message.client_id
        message_data = internal_message.data

        if message_data:
            fruit, amount = message_data 
            self._process_data(client_id, fruit, amount)
        else:
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
