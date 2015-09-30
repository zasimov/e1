from e1 import controller


class Router(controller.AbstractController):

    def __init__(self, address):
        super(Router, self).__init__(address)
        self._routes = {}

    def register(self, controller):
        self._routes[controller.address] = controller

    def _route(self, destination):
        return self._routes[destination]

    def process(self, msg):
        next_hop = self._route(msg.destination)
        return next_hop.process(msg)

    @property
    def addresses(self):
        return self._routes.keys()
