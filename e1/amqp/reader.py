import json
import logging
import time

import kombu
from kombu import mixins

from e1 import message
from e1 import reader


LOG = logging.getLogger(__name__)


class AMQPReader(mixins.ConsumerMixin, reader.Reader):

    def __init__(self, controller,
                 endpoint, exchange, queue, routing_key,
                 redelivered_wait_time=10):
        super(AMQPReader, self).__init__()
        self.connection = kombu.Connection(endpoint)
        self._exchg = exchange
        self._routing_key = routing_key
        exchange = kombu.Exchange(exchange, type='topic')
        queue = kombu.Queue(queue,
                            exchange,
                            routing_key=routing_key)
        self._queue = queue
        self._controller = controller
        self._redelivered_wait_time = redelivered_wait_time

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self._queue],
                         callbacks=[self.process_task])]

    def _redelivered_sleep(self):
        LOG.info('Sleep %s before process redelivered message',
                 self._redelivered_wait_time)
        time.sleep(self._redelivered_wait_time)

    def _process_redelivered(self, amqp_message):
        if amqp_message.delivery_info.get('redelivered', False):
            self._redelivered_sleep()
        return False

    def _requeue(self, amqp_message):
        if amqp_message.delivery_info.get('redelivered', False):
            LOG.error('Reject message %s', amqp_message.delivery_info['delivery_tag'])
            amqp_message.reject()
        else:
            LOG.error('Requeue message %s', amqp_message.delivery_info['delivery_tag'])
            amqp_message.requeue()

    def process_task(self, body, amqp_message):
        if self._process_redelivered(amqp_message):
            return
        msg = message.parse(body)
        try:
            self._controller.process(msg)
            amqp_message.ack()
        except Exception as e:
            LOG.error('Error occured on message processing')
            LOG.exception(e)
            try:
                self._requeue(amqp_message)
            except Exception as e:
                LOG.error('Cannot requeue message')
                LOG.exception(e)

    def print_run_string(self):
        print 'Run services %s on %s:%s' % (
            self._controller.addresses,
            self._exchg,
            self._routing_key)

    def register(self, registry):
        for address in self._controller.addresses:
            registry.register_service(address,
                                      self._exchg,
                                      self._routing_key)

    def unregister(self, registry):
        for address in self._controller.addresses:
            registry.unregister_service(address,)
