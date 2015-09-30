import abc
import functools
import logging

from e1 import controller
from e1 import dispatcher
from e1.dispatcher import service, rpc, response, exception
from e1 import message


LOG = logging.getLogger(__name__)


class MethodNotFound(Exception):
    def __init__(self, entry_point):
        self._entry_point = entry_point
        super(MethodNotFound, self).__init__(
            'Method \'%s\' not found.' % str(self._entry_point))


class AbstractApplicator(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def apply(self, f, message):
        pass


class DirectApplicator(AbstractApplicator):

    def apply(self, f, message):
        return f(message)


class ArgsApplicator(AbstractApplicator):

    def _cast_names(self, payload):
        args = {}
        for key, value in payload.items():
            key = key.replace('-', '_')
            args[key] = value
        return args

    def apply(self, f, msg):
        """Apply payload to method

        For responses adds cross to arguments and rewrite existing
        values.
        """
        args = self._cast_names(msg.payload)
        if ((isinstance(msg, message.ResponseMessage) and
             not isinstance(msg, message.ExceptionMessage)) and
             msg.cross):
            args.update(self._cast_names(msg.cross))
        return f(**args)


class Service(controller.AbstractMessageRouter):

    DISPATCHER_CLS = dispatcher.Dispatcher
    __dispatcher = None

    APPLICATOR_CLS = ArgsApplicator

    @classmethod
    def get_dispatcher(cls):
        if cls.__dispatcher is None:
            cls.__dispatcher = cls.DISPATCHER_CLS(cls)
        return cls.__dispatcher

    def _get_handler(self, entry_point, fmap, default=None):
        """ Returns not-null handler or raises Exception """
        try:
            handlers = self.get_dispatcher().get_handlers(entry_point)
        except dispatcher.HandlersNotFound as e:
            raise MethodNotFound(e.entry_point)
        handler = fmap(handlers)
        if handler is None:
            if default:
                return default
            raise MethodNotFound(entry_point)
        return functools.partial(handler, self)

    def _selfy(self, entry_point):
        service = ('self'
                   if entry_point.service == self.address
                   else entry_point.service)
        return message.EntryPoint(service,
                                  entry_point.method)

    def _apply(self, f, message):
        applicator = self.APPLICATOR_CLS()
        return applicator.apply(f, message)

    def _call(self, request_message):
        entry_point = request_message.destination
        entry_point = self._selfy(entry_point)
        handler = self._get_handler(entry_point,
                                    lambda handlers: handlers.rpc_handler)
        return self._apply(handler, request_message)

    def _process_response(self, response_message):
        if response_message.destination.method is not None:
            entry_point = message.EntryPoint(dispatcher.SELF_SERVICE,
                                             response_message.destination.method)
            handler = self._get_handler(entry_point,
                                        lambda handlers: handlers.rpc_handler,
                                        self._default_response_handler)
        else:
            entry_point = response_message.source
            entry_point = self._selfy(entry_point)
            handler = self._get_handler(entry_point,
                                        lambda handlers: handlers.response_handler,
                                        functools.partial(self._default_response_handler,
                                                          response_message))
        return self._apply(handler, response_message)

    def _process_exception(self, exception_message):
        entry_point = exception_message.source
        entry_point = self._selfy(entry_point)
        handler = self._get_handler(entry_point,
                                    lambda handlers: handlers.exception_handler,
                                    functools.partial(self._default_exception_handler,
                                                      exception_message))

        return self._apply(handler, exception_message)

    def _default_response_handler(self, response_message, **kwargs):
        LOG.error('Ignore response from %s using default handler',
                  str(response_message.source))

    def _default_exception_handler(self, exception_message, **kwargs):
        LOG.error('Ignore exception %s from %s using default handler',
                  exception_message.exception,
                  str(exception_message.source))

    def print_table(self):
        print '-' * 79
        print 'Handlers table for {} service'.format(self.address)
        print '-' * 79
        self.get_dispatcher().print_table()
        print '-' * 79
