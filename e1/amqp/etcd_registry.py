import etcd
import json

from e1.amqp import registry
from e1.amqp import endpoint


class EtcdRegistry(registry.AbstractRegistry):

    KEY = '/services/'

    def __init__(self, host, port=4001):
        super(EtcdRegistry, self).__init__()
        self._client = etcd.Client(host=host, port=port)

    def _service_key(self, service):
        return self.KEY + service

    def _register(self, service, route):
        self._client.write(self._service_key(service),
                           json.dumps({'exchange': route.exchange,
                                       'routing_key': route.routing_key}))

    def unregister_service(self, service):
        self._client.delete(self._service_key(service))

    def get(self, service):
        ep = (self
              ._client
              .read(self._service_key(service))
              .value)
        ep = json.loads(ep)
        return endpoint.Endpoint(ep['exchange'],
                                 ep['routing_key'])
