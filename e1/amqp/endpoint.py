
class Endpoint(object):

    def __init__(self, exchange, routing_key=None):
        self.exchange = exchange
        self.routing_key = routing_key
