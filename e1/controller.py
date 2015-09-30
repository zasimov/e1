import abc
import logging
import uuid

from e1 import message


LOG = logging.getLogger(__name__)


class AbstractController(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, address):
        super(AbstractController, self).__init__()
        self.address = address

    def controller_entry_point(self, method):
        return message.EntryPoint(self.address, method)

    @abc.abstractmethod
    def process(self, message):
        """ message - Message class or subclass of Message """
        pass


class Controller(AbstractController):

    def __init__(self, address, writer):
        super(Controller, self).__init__(address)
        self.writer = writer

    def _parse_input_message(self, raw_input_message):
        return message.parse(raw_input_message)

    def _make_response(self, input_message, payload):
        return message.ResponseMessage(message_id=input_message.message_id,
                                       source=input_message.destination,
                                       destination=input_message.reply_to,
                                       payload=payload,
                                       cross=input_message.cross)

    def _make_exception(self, input_message, e):
        class_name = str(e.__class__.__name__)
        return message.ExceptionMessage(message_id=input_message.message_id,
                                        source=input_message.destination,
                                        destination=input_message.reply_to,
                                        payload={'class_name': class_name,
                                                 'message': e.message},
                                        exception=class_name,
                                        cross=input_message.cross)

    def _send(self, output_message):
        if not output_message.destination.is_nil():
            LOG.info('Send %s to %s from %s',
                     output_message.__class__.__name__,
                     str(output_message.destination),
                     str(output_message.source))
            try:
                return self.writer.write(output_message)
            except Exception as e:
                LOG.error('Cannot send message to %s', str(output_message.destination))
                LOG.exception(e)
                raise

    def _invoke(self, entry_point, payload, cross=None, callback=None):
        request_message = message.RequestMessage(
            message_id=str(uuid.uuid4()),
            source=self.controller_entry_point(None),
            destination=entry_point,
            reply_to=callback,
            payload=payload,
            cross=cross)
        return self._send(request_message)

    def _forward(self, destination, input_message):
        output_message = input_message.copy()
        output_message.destination = destination
        return self._send(msg)

    def _reply_to(self, input_message, payload):
        response = self._make_response(input_message, payload)
        return self._send(response)

    @property
    def addresses(self):
        return [self.address]


class AbstractMessageRouter(Controller):

    __metaclass = abc.ABCMeta

    def process(self, input_message):
        LOG.info('Process %s from %s to %s',
                 input_message.__class__.__name__,
                 str(input_message.source),
                 str(input_message.destination))
        if isinstance(input_message, message.RequestMessage):
            return self._process_request(input_message)
        else:
            if isinstance(input_message, message.ExceptionMessage):
                return self._process_exception(input_message)
            else:
                return self._process_response(input_message)

    def _process_request(self, request_message):
        try:
            payload = self._call(request_message)
            output_message = self._make_response(request_message,
                                                 payload)
        except Exception as e:
            LOG.error('Error occured during process %s from %s',
                      request_message.__class__.__name__,
                      str(request_message.source))
            LOG.exception(e)
            output_message = self._make_exception(request_message, e)
        return self._send(output_message)

    @abc.abstractmethod
    def _call(self, request_message):
        pass

    @abc.abstractmethod
    def _process_response(self, response_message):
        pass

    @abc.abstractmethod
    def _process_exception(self, exception_message):
        pass
