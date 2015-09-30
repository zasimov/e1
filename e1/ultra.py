import uuid

from e1.amqp import etcd_registry
from e1 import message
from e1.amqp import writer


class Client(object):

    def __init__(self, endpoint, discovery_host, discovery_port=4001):
        super(Client, self).__init__()
        discovery = etcd_registry.EtcdRegistry(
            host=discovery_host,
            port=discovery_port)
        self._writer = writer.AMQPWriter(endpoint, discovery)

    def cast(self, service, method, payload, cross=None):
        destination = message.EntryPoint(service, method)
        reply_to = message.EntryPoint.nil()
        request_message = message.RequestMessage(
            message_id=str(uuid.uuid4()),
            source=message.EntryPoint.nil(),
            destination=destination,
            reply_to=reply_to,
            payload=payload,
            cross=cross)
        return self._writer.write(request_message)

    def call(self, service, method, payload, reply_to_service, reply_to_method, cross=None):
        destination = message.EntryPoint(service, method)
        reply_to = message.EntryPoint(reply_to_service, reply_to_method)
        request_message = message.RequestMessage(
            message_id=str(uuid.uuid4()),
            source=message.EntryPoint.nil(),
            destination=destination,
            reply_to=reply_to,
            payload=payload,
            cross=cross)
        return self._writer.write(request_message)


class World(object):

    def __init__(self, client):
        self._client = client

    def __getattr__(self, service):
        return Service(self._client, service)


class Service(object):

    def __init__(self, client, service):
        self._client = client
        self._service = service

    @property
    def client(self):
        return self._client

    @property
    def service(self):
        return self._service

    def __getattr__(self, method):
        return MethodProxy(self, method)


class MethodProxy(object):

    def __init__(self, service_proxy, name):
        self._service_proxy = service_proxy
        self._name = name

    @property
    def service(self):
        return self._service_proxy.service

    @property
    def name(self):
        return self._name

    def __call__(self, **kwargs):
        kwargs = kwargs.copy()
        callback = kwargs.pop('callback', None)
        if isinstance(callback, Service):
            callback = MethodProxy(callback, None)
        elif callback and not isinstance(callback, MethodProxy):
            raise TypeError('callback must be MethodProxy or Service')
        cross = kwargs.pop('cross', None)
        return self._service_proxy.client.call(service=self._service_proxy.service,
                                               method=self._name,
                                               payload=kwargs,
                                               reply_to_service=callback.service if callback else None,
                                               reply_to_method=callback.name if callback else None,
                                               cross=cross)
