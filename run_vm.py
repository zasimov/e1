from e1 import ultra

endpoint = 'amqp://guest:guest@rabbitmq:5672//'

c = ultra.Client(endpoint, 'etcd')

w = ultra.World(c)

w.network.create_network(name='my_network_x',
                         cross={'name': 'xvm',
                                'flavor': 'medium',
                                'image': 'coreos-stable'},
                         callback=w.vm)
