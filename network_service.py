from e1.amqp import reader
from e1.amqp import etcd_registry
from e1.amqp import writer
from e1 import service

from neutronclient.neutron import client as neutron_client


NETWORK_SERVICE_ADDRESS = 'network'

NEUTRON_VERSION_API = '2.0'


@service.service
class NetworkService(service.Service):

    def __init__(self, address, writer):
        super(NetworkService, self).__init__(
            address, writer)
        self._client = neutron_client.Client(
            NEUTRON_VERSION_API,
            auth_url='http://172.18.12.103:5000/v2.0',
            username='zasimov',
            tenant_name='zasimov',
            password='zasimov')

    @service.rpc()
    def create_network(self, name):
        raise Exception('bad state')
        net = self._client.create_network(
            {'network': {'name': name}})
        params = {
            'network_id': net['network']['id'],
            'cidr': '192.168.0.1/24',
            'enable_dhcp': True,
            'ip_version': 4}
        self._client.create_subnet({'subnet': params})
        return {'net-id': net['network']['id']}


if __name__ == '__main__':
    from e1 import log
    log.setup_for_debugging()

    endpoint = 'amqp://guest:guest@rabbitmq:5672//'

    discovery = etcd_registry.EtcdRegistry(host='etcd')

    # Common writer. Use discovery service to map service name to
    # exchange and routing key
    writer = writer.AMQPWriter(endpoint, discovery)

    network_service = NetworkService(NETWORK_SERVICE_ADDRESS,
                                     writer)
    network_service.print_table()

    reader = reader.AMQPReader(network_service,
                               endpoint,
                               'test_exchange',
                               'network_queue',
                               'network_service_routing_key')
    reader.register(discovery)
    reader.print_run_string()
    try:
        reader.run()
    except:
        reader.unregister(discovery)
        raise
