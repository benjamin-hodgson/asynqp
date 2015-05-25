'''
Example async consumer and publisher that will reconnect
automatically when a connection to rabbitmq is broken and
restored.


.. note::

    No attempt is made to re-send messages that are generated
    while the connection is down.
'''
import asyncio
import asynqp
from asyncio.futures import InvalidStateError


# Global variables are ugly, but this is a simple example
CHANNELS = []
CONNECTION = None
CONSUMER = None
PRODUCER = None


@asyncio.coroutine
def setup_connection(loop):
    # connect to the RabbitMQ broker
    connection = yield from asynqp.connect('localhost',
                                           5672,
                                           username='guest',
                                           password='guest')

    return connection


@asyncio.coroutine
def setup_queue_and_exchange(connection):
    # Open a communications channel
    channel = yield from connection.open_channel()

    # Create a queue and an exchange on the broker
    exchange = yield from channel.declare_exchange('test.exchange', 'direct')
    queue = yield from channel.declare_queue('test.queue')

    # Save a reference to each channel so we can close it later
    CHANNELS.append(channel)

    # Bind the queue to the exchange, so the queue will get messages published to the exchange
    yield from queue.bind(exchange, 'routing.key')

    return exchange, queue


@asyncio.coroutine
def setup_consumer(connection):
    # callback will be called each time a message is received from the queue
    def callback(msg):
        print('Received: {}'.format(msg.body))
        msg.ack()

    _, queue = yield from setup_queue_and_exchange(connection)

    # connect the callback to the queue
    consumer = yield from queue.consume(callback)
    return consumer


@asyncio.coroutine
def setup_producer(connection):
    '''
    The producer will live as an asyncio.Task
    to stop it call Task.cancel()
    '''
    exchange, _ = yield from setup_queue_and_exchange(connection)

    count = 0
    while True:
        msg = asynqp.Message('Message #{}'.format(count))
        exchange.publish(msg, 'routing.key')
        yield from asyncio.sleep(1)
        count += 1


@asyncio.coroutine
def start(loop):
    '''
    Creates a connection, starts the consumer and producer.
    If it fails, it will attempt to reconnect after waiting
    1 second
    '''
    global CONNECTION
    global CONSUMER
    global PRODUCER
    try:
        CONNECTION = yield from setup_connection(loop)
        CONSUMER = yield from setup_consumer(CONNECTION)
        PRODUCER = loop.create_task(setup_producer(CONNECTION))
    # Multiple exceptions may be thrown, ConnectionError, OsError
    except Exception:
        print('failed to connect, trying again.')
        yield from asyncio.sleep(1)
        loop.create_task(start(loop))


@asyncio.coroutine
def stop():
    '''
    Cleans up connections, channels, consumers and producers
    when the connection is closed.
    '''
    global CHANNELS
    global CONNECTION
    global PRODUCER
    global CONSUMER

    yield from CONSUMER.cancel()  # this is a coroutine
    PRODUCER.cancel()  # this is not

    for channel in CHANNELS:
        yield from channel.close()
    CHANNELS = []

    if CONNECTION is not None:
        try:
            yield from CONNECTION.close()
        except InvalidStateError:
            pass  # could be automatically closed, so this is expected
        CONNECTION = None


def connection_lost_handler(loop, context):
    '''
    Here we setup a custom exception handler to listen for
    ConnectionErrors.

    The exceptions we can catch follow this inheritance scheme

        - ConnectionError - base
            |
            - asynqp.exceptions.ConnectionClosedError - connection closed properly
                |
                - asynqp.exceptions.ConnectionLostError - closed unexpectedly
    '''
    exception = context.get('exception')
    if isinstance(exception, asynqp.exceptions.ConnectionClosedError):
        print('Connection lost -- trying to reconnect')
        # close everything before recpnnecting
        close_task = loop.create_task(stop())
        asyncio.wait_for(close_task, None)
        # reconnect
        loop.create_task(start(loop))
    else:
        # default behaviour
        loop.default_exception_handler(context)


loop = asyncio.get_event_loop()
loop.set_exception_handler(connection_lost_handler)
loop.create_task(start(loop))
loop.run_forever()
