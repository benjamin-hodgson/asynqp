import asyncio
import asynqp
import socket
import contexts
from .util import run_briefly


class ConnectionContext:
    def given_a_connection(self):
        self.loop = asyncio.get_event_loop()
        self.connection = self.loop.run_until_complete(asyncio.wait_for(asynqp.connect(), 0.2))

    def cleanup_the_connection(self):
        self.loop.run_until_complete(asyncio.wait_for(self.connection.close(), 0.2))


class ChannelContext(ConnectionContext):
    def given_a_channel(self):
        self.channel = self.loop.run_until_complete(asyncio.wait_for(self.connection.open_channel(), 0.2))

    def cleanup_the_channel(self):
        self.loop.run_until_complete(asyncio.wait_for(self.channel.close(), 0.2))


class BoundQueueContext(ChannelContext):
    def given_a_queue_bound_to_an_exchange(self):
        self.loop.run_until_complete(asyncio.wait_for(self.setup(), 0.5))

    def cleanup_the_queue_and_exchange(self):
        self.loop.run_until_complete(asyncio.wait_for(self.teardown(), 0.3))

    @asyncio.coroutine
    def setup(self):
        self.queue = yield from self.channel.declare_queue('my.queue', exclusive=True)
        self.exchange = yield from self.channel.declare_exchange('my.exchange', 'fanout')

        yield from self.queue.bind(self.exchange, 'doesntmatter')

    @asyncio.coroutine
    def teardown(self):
        yield from self.queue.delete(if_unused=False, if_empty=False)
        yield from self.exchange.delete(if_unused=False)


class WhenConnectingToRabbit:
    def given_the_loop(self):
        self.loop = asyncio.get_event_loop()

    def when_I_connect(self):
        self.connection = self.loop.run_until_complete(asyncio.wait_for(asynqp.connect(), 0.2))

    def it_should_connect(self):
        assert self.connection is not None

    def cleanup_the_connection(self):
        self.loop.run_until_complete(asyncio.wait_for(self.connection.close(), 0.2))


class WhenConnectingToRabbitWithAnExistingSocket:
    def given_the_loop(self):
        self.loop = asyncio.get_event_loop()
        self.sock = socket.create_connection(("localhost", 5672))

    def when_I_connect(self):
        self.connection = self.loop.run_until_complete(asyncio.wait_for(asynqp.connect(sock=self.sock), 0.2))

    def it_should_connect(self):
        assert self.connection is not None

    def cleanup_the_connection(self):
        self.loop.run_until_complete(asyncio.wait_for(self.connection.close(), 0.2))
        self.sock.close()


class WhenOpeningAChannel(ConnectionContext):
    def when_I_open_a_channel(self):
        self.channel = self.loop.run_until_complete(asyncio.wait_for(self.connection.open_channel(), 0.2))

    def it_should_give_me_the_channel(self):
        assert self.channel is not None

    def cleanup_the_channel(self):
        self.loop.run_until_complete(asyncio.wait_for(self.channel.close(), 0.2))


class WhenDeclaringAQueue(ChannelContext):
    ARGUMENTS = {'x-expires': 300, 'x-message-ttl': 1000, 'x-table-test': {'a': [1, 'a', {}, []], 'c': 1}}

    def when_I_declare_a_queue(self):
        coro = self.channel.declare_queue('my.queue', exclusive=True, arguments=WhenDeclaringAQueue.ARGUMENTS)
        self.queue = self.loop.run_until_complete(asyncio.wait_for(coro, 0.2))

    def it_should_have_the_correct_queue_name(self):
        assert self.queue.name == 'my.queue'

    def it_should_have_the_correct_attributes_in_rabbitmq(self):
        assert self.queue.arguments == WhenDeclaringAQueue.ARGUMENTS

    def cleanup_the_queue(self):
        self.loop.run_until_complete(asyncio.wait_for(self.queue.delete(if_unused=False, if_empty=False), 0.2))


class WhenDeclaringAnExchange(ChannelContext):
    def when_I_declare_an_exchange(self):
        self.exchange = self.loop.run_until_complete(asyncio.wait_for(self.channel.declare_exchange('my.exchange', 'fanout'), 0.2))

    def it_should_have_the_correct_name(self):
        assert self.exchange.name == 'my.exchange'

    def cleanup_the_exchange(self):
        self.loop.run_until_complete(asyncio.wait_for(self.exchange.delete(if_unused=False), 0.2))


class WhenPublishingAndGettingAShortMessage(BoundQueueContext):
    def given_I_published_a_message(self):
        self.message = asynqp.Message('here is the body')
        self.exchange.publish(self.message, 'routingkey')

    def when_I_get_the_message(self):
        self.result = self.loop.run_until_complete(asyncio.wait_for(self.queue.get(), 0.2))

    def it_should_return_my_message(self):
        assert self.result == self.message


class WhenConsumingAShortMessage(BoundQueueContext):
    def given_a_consumer(self):
        self.message = asynqp.Message('this is my body')
        self.message_received = asyncio.Future()
        self.loop.run_until_complete(asyncio.wait_for(self.queue.consume(self.message_received.set_result), 0.2))

    def when_I_publish_a_message(self):
        self.exchange.publish(self.message, 'routingkey')
        self.loop.run_until_complete(asyncio.wait_for(self.message_received, 0.2))

    def it_should_deliver_the_message_to_the_consumer(self):
        assert self.message_received.result() == self.message


class WhenIStartAConsumerWithAMessageWaiting(BoundQueueContext):
    def given_a_published_message(self):
        self.message = asynqp.Message('this is my body')
        self.exchange.publish(self.message, 'routingkey')

    def when_I_start_a_consumer(self):
        self.message_received = asyncio.Future()
        self.loop.run_until_complete(asyncio.wait_for(self.start_consumer(), 0.2))

    def it_should_deliver_the_message_to_the_consumer(self):
        assert self.message_received.result() == self.message

    @asyncio.coroutine
    def start_consumer(self):
        yield from self.queue.consume(self.message_received.set_result)
        yield from self.message_received


class WhenIStartAConsumerWithSeveralMessagesWaiting(BoundQueueContext):
    def given_published_messages(self):
        self.message1 = asynqp.Message('one')
        self.message2 = asynqp.Message('one')
        self.exchange.publish(self.message1, 'routingkey')
        self.exchange.publish(self.message2, 'routingkey')

        self.received = []

    def when_I_start_a_consumer(self):
        self.loop.run_until_complete(asyncio.wait_for(self.start_consumer(), 0.3))

    def it_should_deliver_the_messages_to_the_consumer(self):
        assert self.received == [self.message1, self.message2]

    @asyncio.coroutine
    def start_consumer(self):
        yield from self.queue.consume(self.received.append)
        yield from asyncio.sleep(0.05)  # possibly flaky


class WhenPublishingAndGettingALongMessage(BoundQueueContext):
    def given_a_multi_frame_message_and_a_consumer(self):
        frame_max = self.connection.connection_info['frame_max']
        body1 = "a" * (frame_max - 8)
        body2 = "b" * (frame_max - 8)
        body3 = "c" * (frame_max - 8)
        body = body1 + body2 + body3
        self.msg = asynqp.Message(body)

    def when_I_publish_and_get_the_message(self):
        self.exchange.publish(self.msg, 'routingkey')
        self.result = self.loop.run_until_complete(asyncio.wait_for(self.queue.get(), 0.2))

    def it_should_return_my_message(self):
        assert self.result == self.msg


class WhenPublishingAndConsumingALongMessage(BoundQueueContext):
    def given_a_multi_frame_message(self):
        frame_max = self.connection.connection_info['frame_max']
        body1 = "a" * (frame_max - 8)
        body2 = "b" * (frame_max - 8)
        body3 = "c" * (frame_max - 8)
        body = body1 + body2 + body3
        self.msg = asynqp.Message(body)

        self.message_received = asyncio.Future()
        self.loop.run_until_complete(asyncio.wait_for(self.queue.consume(self.message_received.set_result), 0.2))

    def when_I_publish_and_get_the_message(self):
        self.exchange.publish(self.msg, 'routingkey')
        self.loop.run_until_complete(asyncio.wait_for(self.message_received, 0.2))

    def it_should_deliver_the_message_to_the_consumer(self):
        assert self.message_received.result() == self.msg


class WhenBasicCancelIsInterleavedWithAnotherMethod(BoundQueueContext):
    def given_I_have_started_a_consumer(self):
        self.consumer = self.loop.run_until_complete(asyncio.wait_for(self.queue.consume(lambda x: None), 0.2))

    def when_I_cancel_the_consumer_and_also_get_a_message(self):
        self.consumer.cancel()
        self.exception = contexts.catch(self.loop.run_until_complete, asyncio.wait_for(self.queue.get(), 0.2))

    def it_should_not_throw(self):
        assert self.exception is None


class WhenPublishingWithUnsetLoop:

    def given_I_have_a_queue(self):
        @asyncio.coroutine
        def set_up():
            self.connection = yield from asynqp.connect(loop=self.loop)
            self.channel = yield from self.connection.open_channel()
            self.exchange = yield from self.channel.declare_exchange(
                '', 'direct')
            self.queue = yield from self.channel.declare_queue(
                durable=False,
                exclusive=True,
                auto_delete=True)
        self.loop = asyncio.get_event_loop()
        asyncio.set_event_loop(None)
        self.loop.run_until_complete(set_up())

    def when_I_publish_the_message(self):
        message = asynqp.Message(b"Test message")
        self.exchange.publish(message, self.queue.name)

    def it_should_return_my_message(self):
        for retry in range(10):
            msg = self.loop.run_until_complete(self.queue.get(no_ack=True))
            if msg is not None:
                break
        assert msg.body == b"Test message"

    def cleanup_loop(self):
        @asyncio.coroutine
        def tear_down():
            yield from self.channel.close()
            yield from self.connection.close()
        self.loop.run_until_complete(tear_down())
        asyncio.set_event_loop(self.loop)


class WhenConsumingWithUnsetLoop:

    def given_I_published_a_message(self):
        @asyncio.coroutine
        def set_up():
            self.connection = yield from asynqp.connect(loop=self.loop)
            self.channel = yield from self.connection.open_channel()
            self.exchange = yield from self.channel.declare_exchange(
                '', 'direct')
            self.queue = yield from self.channel.declare_queue(
                durable=False,
                exclusive=True,
                auto_delete=True)
        self.loop = asyncio.get_event_loop()
        asyncio.set_event_loop(None)
        self.loop.run_until_complete(set_up())

        message = asynqp.Message(b"Test message")
        self.exchange.publish(message, self.queue.name)

    def when_I_consume_a_message(self):
        self.result = []
        consumer = self.loop.run_until_complete(
            self.queue.consume(self.result.append, exclusive=True))
        for retry in range(10):
            run_briefly(self.loop)
            if self.result:
                break
        consumer.cancel()

    def it_should_return_my_message(self):
        assert self.result, "Message not consumed"
        assert self.result[0].body == b"Test message"

    def cleanup_loop(self):
        @asyncio.coroutine
        def tear_down():
            yield from self.channel.close()
            yield from self.connection.close()
        self.loop.run_until_complete(tear_down())
        asyncio.set_event_loop(self.loop)


class WhenISendZeroMessage(BoundQueueContext):
    def given_an_empty_message(self):
        self.message = asynqp.Message('')
        self.exchange.publish(self.message, 'routingkey')

    def when_I_start_a_consumer(self):
        self.message_received = asyncio.Future()
        self.loop.run_until_complete(asyncio.wait_for(self.start_consumer(), 0.2))

    def it_should_deliver_the_message_to_the_consumer(self):
        assert self.message_received.result() == self.message

    @asyncio.coroutine
    def start_consumer(self):
        yield from self.queue.consume(self.message_received.set_result)
        yield from self.message_received
