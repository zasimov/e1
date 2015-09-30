import kombu
from kombu import pools

from e1.amqp import endpoint
from e1 import writer


class KombuAMQPWriter(writer.Writer):

    def __init__(self, endpoint):
        super(KombuAMQPWriter, self).__init__()
        self._conn = kombu.Connection(endpoint)

    def _route(self, message):
        pass

    def write(self, message):
        amqp_route = self._route(message)
        payload = message.to_json()
        with pools.producers[self._conn].acquire(block=True) as producer:
            producer.publish(payload,
                             exchange=amqp_route.exchange,
                             routing_key=amqp_route.routing_key)


class DirectAMQPWriter(KombuAMQPWriter):

    def __init__(self, endpoint, exchange, routing_key):
        super(DirectAMQPWriter, self).__init__(endpoint)
        self._ep = endpoint.Endpoint(exchange, routing_key)

    def _route(self, message):
        return self._ep


class AMQPWriter(KombuAMQPWriter):

    def __init__(self, endpoint, registry):
        super(AMQPWriter, self).__init__(endpoint)
        self._registry = registry

    def _route(self, msg):
        return self._registry.get(msg.destination.service)
