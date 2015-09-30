from novaclient.v1_1 import client as nova_v1_1_client

from e1.amqp import reader
from e1.amqp import etcd_registry
from e1.amqp import writer
from e1 import service


VM_SERVICE_ADDRESS = 'vm'


@service.service
class VMService(service.Service):

    def __init__(self, address, writer):
        super(VMService, self).__init__(address, writer)
        self._client = nova_v1_1_client.Client(
            username='zasimov',
            api_key='zasimov',
            project_id='zasimov',
            auth_url='http://172.18.12.103:5000/v2.0')

    @service.response('network', 'create_network')
    def run_vm_in_network(self, name, flavor, image, net_id):
        """Run if network service sends response from network.create_network"""
        flavor = self._client.flavors.find(name=flavor)
        image = self._client.images.find(name=image)
        server = self._client.servers.create(
            name,
            image,
            flavor,
            nics=[{'net-id': net_id}])
        return {'instance_id': server.id}

    @service.exception('network', 'create_network')
    def handle_create_network_error(self, class_name, message):
        print 'Handle create_network error', class_name, message


if __name__ == '__main__':
    from e1 import log
    log.setup_for_debugging()

    endpoint = 'amqp://guest:guest@rabbitmq:5672//'

    discovery = etcd_registry.EtcdRegistry(host='etcd')

    # Common writer. Use discovery service to map service name to
    # exchange and routing key
    writer = writer.AMQPWriter(endpoint, discovery)

    vm_service = VMService(VM_SERVICE_ADDRESS,
                           writer)
    vm_service.print_table()

    reader = reader.AMQPReader(vm_service,
                               endpoint,
                               'test_exchange',
                               'new_queue',
                               'new_service_routing_key')
    reader.register(discovery)
    reader.print_run_string()
    try:
        reader.run()
    except:
        reader.unregister(discovery)
        raise
