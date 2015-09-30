# Map destination address to exchange

import abc

from e1.amqp import endpoint


class AbstractRegistry(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def _register(self, service, route):
        pass

    def register_service(self, service_name, exchange, routing_key):
        ep = endpoint.Endpoint(exchange, routing_key)
        return self._register(service_name, ep)

    @abc.abstractmethod
    def unregister_service(self, service):
        pass

    def get(self, service_name):
        pass


class Registry(AbstractRegistry):

    def __init__(self):
        super(Registry, self).__init__()
        self._registry = {}

    def _register(self, service, ep):
        self._registry[service] = ep

    def unregister_service(self, service):
        del self._register[service]

    def get(self, service_name):
        return self._registry[service_name]
