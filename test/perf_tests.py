import asyncio
import asynqp
import contexts
from .integration_tests import BoundQueueContext


class WhenConsumingLoadsOfMessages(BoundQueueContext):
    def establish_one_thousand_messages_in_the_queue(self):
        self.msg_count = 1000
        self.condition = asyncio.Condition()
        self.received = []
        for x in range(self.msg_count):
            self.exchange.publish(asynqp.Message(str(x)), 'not_used')

    def because_i_start_the_consumer(self):
        self.time = contexts.time(self.loop.run_until_complete, self.consume())

    def it_should_consume_the_messages_reasonably_quickly(self):
        assert self.time < 1.5

    @asyncio.coroutine
    def consume(self):
        yield from self.queue.consume(lambda msg: asyncio.async(self.ack(msg)))
        yield from self.condition.acquire()
        yield from self.condition.wait_for(lambda: len(self.received) == self.msg_count)

    @asyncio.coroutine
    def ack(self, msg):
        msg.ack()
        self.received.append(msg)
        yield from self.condition.acquire()
        self.condition.notify()
        self.condition.release()
