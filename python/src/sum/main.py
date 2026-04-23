import os
import logging
import threading
import zlib
import signal

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
        self.data_per_client = {}
        self.lock = threading.Lock()
        self._sigterm_prev_handler = None
        self._input_queue = None
        self._control_exchange = None
        self._control_exchange_publisher = None
        self._data_output_exchanges = []
        self._control_output_exchanges = []

    def __enter__(self):
        self._sigterm_prev_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        return self

    def __exit__(self, exc_type, exc, tb):
        self._restore_sigterm_handler()
        self.close()
        return False

    def start(self):
        t_control = threading.Thread(target=self._run_control_consumer, daemon=True)
        t_control.start()
        self._run_data_consumer()
        t_control.join()

    def stop(self):
        if self._input_queue:
            self._input_queue.stop_consuming()
        if self._control_exchange:
            self._control_exchange.stop_consuming()

    def close(self):
        if self._control_exchange_publisher:
            self._control_exchange_publisher.close()
        if self._control_exchange:
            self._control_exchange.close()
        if self._input_queue:
            self._input_queue.close()
        for exchange in self._data_output_exchanges:
            exchange.close()
        for exchange in self._control_output_exchanges:
            exchange.close()

    def _handle_sigterm(self, signum, frame):
        logging.info("SIGTERM received, stopping sum consumers")
        self.stop()

    def _restore_sigterm_handler(self):
        if self._sigterm_prev_handler is not None:
            signal.signal(signal.SIGTERM, self._sigterm_prev_handler)

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
        total_fruits_of_client[fruit] = current_fruit_item + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Processing EOF for client {client_id}")
        if client_id not in self.data_per_client:
            return []
        items = list(self.data_per_client[client_id].values())
        del self.data_per_client[client_id]
        return items

    def _send_eof(self, client_id, items, data_output_exchanges):
        for final_fruit_item in items:
            agg_index = self._get_aggregator_index(final_fruit_item.fruit)
            data_output_exchanges[agg_index].send(
                InternalMessage(
                    client_id=client_id,
                    data=[final_fruit_item.fruit, final_fruit_item.amount]
                ).serialize()
            )
        logging.info(f"Finished processing EOF for client {client_id}")
        # EOF a todos los Aggs SIEMPRE, haya o no haya tenido datos
        for data_output_exchange in data_output_exchanges:
            data_output_exchange.send(
                InternalMessage(client_id=client_id, data=None).serialize()
            )
        logging.info(f"Cleaned data for client {client_id}")    

    # Este metodo se ejecuta en el ciclo de vida del hilo, por eso se inicializan los exchanges dentro del metodo y no en el constructor, para evitar problemas de inicializacion
    def _run_data_consumer(self):
        self._input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self._control_exchange_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE,
            [f"{SUM_CONTROL_EXCHANGE}_{i}" for i in range(SUM_AMOUNT) if i != ID]
        )
        self._data_output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            ) for i in range(AGGREGATION_AMOUNT)
        ]
        data_output_exchanges = self._data_output_exchanges
        control_exchange_publisher = self._control_exchange_publisher

        def callback(message, ack, nack):
            items = None
            with self.lock:
                internal_message = InternalMessage.deserialize(message)
                client_id = internal_message.client_id
                if internal_message.data:
                    fruit, amount = internal_message.data
                    self._process_data(client_id, fruit, amount)
                else:
                    logging.info(f"Got EOF from gateway for client {client_id}, flushing and propagating")
                    items = self._process_eof(client_id)

            if items is not None:
                self._send_eof(client_id, items, data_output_exchanges)
                control_exchange_publisher.send(
                    InternalMessage(client_id=client_id, data=None).serialize()
                )
            ack()

        logging.info("Data consumer started")
        self._input_queue.start_consuming(callback)

    # Este metodo se ejecuta en el ciclo de vida del hilo, por eso se inicializan los exchanges dentro del metodo y no en el constructor, para evitar problemas de inicializacion
    def _run_control_consumer(self):
        self._control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )
        self._control_output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            ) for i in range(AGGREGATION_AMOUNT)
        ]
        data_output_exchanges = self._control_output_exchanges

        def callback(message, ack, nack):
            items = None
            with self.lock:
                internal_message = InternalMessage.deserialize(message)
                client_id = internal_message.client_id
                if internal_message.data is None:
                    logging.info(f"Got EOF from control exchange for client {client_id}, flushing")
                    items = self._process_eof(client_id)

            if items is not None:
                self._send_eof(client_id, items, data_output_exchanges)
            ack()

        logging.info("Control consumer started")
        self._control_exchange.start_consuming(callback)

    # Me devuelve el indice del Aggregator correspondiente a esa fruta, garantiza que:
    # 1) La misma fruta siempre va al mismo Aggregator
    # 2) Las frutas se distribuyen de manera uniforme entre los Aggregators
    def _get_aggregator_index(self, fruit):
        return zlib.crc32(fruit.encode()) % AGGREGATION_AMOUNT

def main():
    logging.basicConfig(level=logging.INFO)
    with SumFilter() as sum_filter:
        sum_filter.start()
    return 0


if __name__ == "__main__":
    main()