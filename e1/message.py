import abc
import json


class EntryPoint(object):
    """ Describes service entry point. """

    def __init__(self, service, method):
        super(EntryPoint, self).__init__()
        self._service = service
        self._method = method

    @property
    def service(self):
        return self._service

    @property
    def method(self):
        return self._method

    def to_dict(self):
        return {'service': self.service,
                'method': self.method}

    def is_nil(self):
        return self.service is None

    def copy(self):
        return EntryPoint(self.service,
                          self.method)

    @classmethod
    def from_dict(cls, d):
        return cls(d['service'],
                   d['method'])

    @classmethod
    def nil(cls):
        return cls(None, None)

    def __str__(self):
        return '{0}.{1}'.format(self.service,
                                self.method)

    def __eq__(self, other):
        if other is None:
            return False
        if not isinstance(other, EntryPoint):
            return False
        return (self.service == other.service and
                self.method == other.method)


class Message(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self,
                 message_id,
                 source,
                 destination,
                 payload,
                 cross):
        super(Message, self).__init__()
        if not isinstance(source, EntryPoint):
            raise TypeError('source must be EntryPoint')
        if not isinstance(destination, EntryPoint):
            raise TypeError('destination must be EntryPoint')
        if not isinstance(payload, dict):
            raise TypeError('payload must be dict, received {}'
                            .format(type(payload)))
        if cross is not None and not isinstance(cross, dict):
            raise TypeError('cross must be dict')
        self.message_id = message_id
        self.source = source
        self.destination = destination
        self.payload = payload
        self.cross = cross

    @abc.abstractmethod
    def is_request(self):
        pass

    def is_response(self):
        return not self.is_request()

    def to_dict(self):
        return {'message_id': self.message_id,
                'source': self.source.to_dict(),
                'destination': self.destination.to_dict(),
                'is_request': self.is_request(),
                'payload': self.payload,
                'cross': self.cross}

    def to_json(self):
        return json.dumps(self.to_dict())

    @abc.abstractmethod
    def copy(self):
        pass


class RequestMessage(Message):

    def __init__(self,
                 message_id,
                 source,
                 destination,
                 reply_to,
                 payload,
                 cross):
        super(RequestMessage, self).__init__(message_id,
                                             source,
                                             destination,
                                             payload,
                                             cross)
        if not isinstance(reply_to, EntryPoint):
            raise TypeError('reply_to must be EntryPoint')
        self.reply_to = reply_to

    def is_request(self):
        return True

    def to_dict(self):
        d = super(RequestMessage, self).to_dict()
        d['reply_to'] = self.reply_to.to_dict()
        return d

    def copy(self):
        return RequestMessage(self.message_id,
                              self.source.copy(),
                              self.destination.copy(),
                              self.reply_to.copy(),
                              self.payload,
                              self.cross)


class ResponseMessage(Message):

    def is_request(self):
        return False

    def copy(self):
        return ResponseMessage(self.message_id,
                               self.source.copy(),
                               self.destination.copy(),
                               self.payload,
                               self.cross)


class ExceptionMessage(ResponseMessage):

    def __init__(self,
                 message_id,
                 source,
                 destination,
                 payload,
                 exception,
                 cross):
        super(ExceptionMessage, self).__init__(message_id,
                                               source,
                                               destination,
                                               payload,
                                               cross)
        self.exception = exception

    def to_dict(self):
        d = super(ExceptionMessage, self).to_dict()
        d['exception'] = self.exception
        return d

    def copy(self):
        return ExceptionMessage(self.message_id,
                                self.source.copy(),
                                self.destination.copy(),
                                self.payload,
                                self.exception,
                                self.cross)



class Parser(object):

    def parse(self, raw_message):
        message = json.loads(raw_message)

        if 'exception' in message:
            return ExceptionMessage(message['message_id'],
                                    EntryPoint.from_dict(message['source']),
                                    EntryPoint.from_dict(message['destination']),
                                    message['payload'],
                                    message['exception'],
                                    message['cross'])
        else:
            if message['is_request']:
                return RequestMessage(message['message_id'],
                                      EntryPoint.from_dict(message['source']),
                                      EntryPoint.from_dict(message['destination']),
                                      EntryPoint.from_dict(message['reply_to']),
                                      message['payload'],
                                      message['cross'])
            else:
                return ResponseMessage(message['message_id'],
                                       EntryPoint.from_dict(message['source']),
                                       EntryPoint.from_dict(message['destination']),
                                       message['payload'],
                                       message['cross'])


def parse(raw_message):
    parser = Parser()
    return parser.parse(raw_message)
