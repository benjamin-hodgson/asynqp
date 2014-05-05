import asyncio
import asynqp


class ConnectionContext:
    def given_a_connection(self):
        self.loop = asyncio.get_event_loop()
        self.connection = self.loop.run_until_complete(asyncio.wait_for(asynqp.connect(), 0.2))

    def cleanup_the_connection(self):
        self.loop.run_until_complete(self.connection.close())


class ChannelContext(ConnectionContext):
    def given_a_channel(self):
        self.channel = self.loop.run_until_complete(asyncio.wait_for(self.connection.open_channel(), 0.2))

    def cleanup_the_channel(self):
        self.loop.run_until_complete(self.channel.close())


class WhenConnectingToRabbit:
    def given_the_loop(self):
        self.loop = asyncio.get_event_loop()

    def when_I_connect(self):
        self.connection = self.loop.run_until_complete(asyncio.wait_for(asynqp.connect(), 0.2))

    def it_should_connect(self):
        assert self.connection is not None

    def cleanup_the_connection(self):
        self.loop.run_until_complete(self.connection.close())


class WhenOpeningAChannel(ConnectionContext):
    def when_I_open_a_channel(self):
        self.channel = self.loop.run_until_complete(asyncio.wait_for(self.connection.open_channel(), 0.2))

    def it_should_give_me_the_channel(self):
        assert self.channel is not None

    def cleanup_the_channel(self):
        self.loop.run_until_complete(self.channel.close())


class WhenDeclaringAQueue(ChannelContext):
    def when_I_declare_a_queue(self):
        self.queue = self.loop.run_until_complete(asyncio.wait_for(self.channel.declare_queue('my.queue', exclusive=True), 0.2))

    def it_should_have_the_correct_queue_name(self):
        assert self.queue.name == 'my.queue'

    def cleanup_the_queue(self):
        self.loop.run_until_complete(self.queue.delete())


class WhenDeclaringAnExchange(ChannelContext):
    def when_I_declare_an_exchange(self):
        self.exchange = self.loop.run_until_complete(asyncio.wait_for(self.channel.declare_exchange('my.exchange', 'fanout'), 0.2))

    def it_should_have_the_correct_name(self):
        assert self.exchange.name == 'my.exchange'


class WhenPublishingAndGettingAShortMessage(ChannelContext):
    def given_I_published_a_message(self):
        self.loop.run_until_complete(asyncio.wait_for(self.setup(), 0.4))

    def when_I_get_the_message(self):
        self.result = self.loop.run_until_complete(asyncio.wait_for(self.queue.get(), 0.2))

    def it_should_return_my_message(self):
        assert self.result == self.message

    def cleanup_the_queue(self):
        self.loop.run_until_complete(self.queue.delete())

    @asyncio.coroutine
    def setup(self):
        self.queue = yield from self.channel.declare_queue('my.queue', exclusive=True)
        self.exchange = yield from self.channel.declare_exchange('my.exchange', 'fanout')

        yield from self.queue.bind(self.exchange, 'doesntmatter')

        self.message = asynqp.Message('here is the body')
        self.exchange.publish(self.message, 'routingkey')
