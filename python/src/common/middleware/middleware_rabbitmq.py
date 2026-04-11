import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareCloseError
class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        res = self.channel.queue_declare(queue=queue_name, exclusive=(queue_name == ''))
        self.queue_name = res.method.queue

    def start_consuming(self, on_message_callback):
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback_wrapper(on_message_callback))
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        try: 
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost while sending message.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while sending message: {str(e)}")

    def close(self):
        try: 
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"An error occurred while closing connection: {str(e)}")
    
    def _callback_wrapper(self, on_message_callback):
        def wrapper(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        return wrapper

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    _EXCHANGE_TYPE = 'direct'
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=self._EXCHANGE_TYPE)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self._receiver_queue = MessageMiddlewareQueueRabbitMQ(host, '')
        for routing_key in routing_keys:
            self._receiver_queue.channel.queue_bind(
                exchange=exchange_name, 
                queue=self._receiver_queue.queue_name, 
                routing_key=routing_key
            )
            
    def send(self, message):
        for routing_key in self.routing_keys:
            try:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
            except pika.exceptions.AMQPConnectionError:
                raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost while sending message.")
            except Exception as e:
                raise MessageMiddlewareMessageError(f"An error occurred while sending message: {str(e)}")
    
    def start_consuming(self, on_message_callback):
        try:
            self._receiver_queue.start_consuming(on_message_callback)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost while starting to consume messages.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while starting to consume messages: {str(e)}")

    def stop_consuming(self):
        try:
            self._receiver_queue.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost while stopping consumption.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while stopping consumption: {str(e)}")

    def close(self):
        self._receiver_queue.close()
        try: 
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"An error occurred while closing connection: {str(e)}")