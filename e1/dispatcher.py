from e1 import message


SELF_SERVICE = 'self'


class EntryType(object):
    rpc_handler = 1
    response_handler = 2
    exception_handler = 3


class Handlers(object):

    def __init__(self,
                 rpc_handler=None,
                 response_handler=None,
                 exception_handler=None):
        self.rpc_handler = rpc_handler
        self.response_handler = response_handler
        self.exception_handler = exception_handler

    def debug_vector(self):
        return '{} {} {}'.format(
            'C' if self.rpc_handler else 'X',
            'R' if self.response_handler else 'X',
            'E' if self.exception_handler else 'X')


class HandlersNotFound(Exception):

    def __init__(self, entry_point):
        self._entry_point = entry_point
        super(HandlersNotFound, self).__init__(
            'Handlers doesn\'t defined for %s' % str(self._entry_point))

    @property
    def entry_point(self):
        return self._entry_point


class Dispatcher(object):

    def __init__(self, service_cls):
        super(Dispatcher, self).__init__()
        self._service_cls = service_cls
        self._handlers = {}

    def get_handlers(self, entry_point):
        """ Return handlers that defined for entry_point. """
        if not isinstance(entry_point, message.EntryPoint):
            raise TypeError('entry_point must be message.EntryPoint')
        try:
            return self._handlers[str(entry_point)]
        except KeyError:
            raise HandlersNotFound(entry_point)

    def _setup(self, entry_point, handlers):
        self._handlers[str(entry_point)] = handlers

    def _ensure_handlers(self, entry_point):
        try:
            handlers = self.get_handlers(entry_point)
        except HandlersNotFound:
            handlers = Handlers()
            self._setup(entry_point, handlers)
        return handlers

    def register_rpc_handler(self, entry_point, f):
        handlers = self._ensure_handlers(entry_point)
        if not callable(f):
            raise TypeError('f must be callbale')
        handlers.rpc_handler = f

    def register_response_handler(self, entry_point, f):
        handlers = self._ensure_handlers(entry_point)
        if not callable(f):
            raise TypeError('f must be callbale')
        handlers.response_handler = f

    def register_exception_handler(self, entry_point, f):
        handlers = self._ensure_handlers(entry_point)
        if not callable(f):
            raise TypeError('f must be callbale')
        handlers.exception_handler = f

    def register(self, method):
        entry_point = message.EntryPoint(method._service,
                                         method._method)
        if method._entry_type == EntryType.rpc_handler:
            return self.register_rpc_handler(entry_point, method)
        elif method._entry_type == EntryType.response_handler:
            return self.register_response_handler(entry_point, method)
        elif method._entry_type == EntryType.exception_handler:
            return self.register_exception_handler(entry_point, method)

    def print_table(self):
        """ Method for debugging. """
        def fill(t, w=50):
            return t + ' ' * (w - len(t))

        for entry_point, handlers in self._handlers.items():
            print fill(entry_point), handlers.debug_vector()


def service(cls):
    """ Traverse class methods and register it in dispatcher """

    for methodname in dir(cls):
        method = getattr(cls, methodname)
        if hasattr(method, '_entry_type'):
            cls.get_dispatcher().register(method)

    return cls


def rpc(entry_name=None):
    def wrapper(func):
        func._entry_type = EntryType.rpc_handler
        func._service = SELF_SERVICE
        func._method = entry_name if entry_name else func.func_name
        return func
    return wrapper


def response(service, method=None):
    def wrapper(func):
        func._entry_type = EntryType.response_handler
        func._service = service
        func._method = method if method else func.func_name
        return func
    return wrapper


def exception(service, method=None):
    def wrapper(func):
        func._entry_type = EntryType.exception_handler
        func._service = service
        func._method = method if method else func.func_name
        return func
    return wrapper
