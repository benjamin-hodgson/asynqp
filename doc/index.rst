.. asynqp documentation master file, created by
   sphinx-quickstart on Wed Jun 11 20:44:10 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

``asynqp``
==========
An AMQP (aka `RabbitMQ <http://www.rabbitmq.com/>`_) client library for :mod:`asyncio`.


Example
-------
::

    import asyncio
    import asynqp


    @asyncio.coroutine
    def send_and_receive():
        # connect to the RabbitMQ broker
        connection = yield from asynqp.connect('localhost', 5672, username='guest', password='guest')

        # Open a communications channel
        channel = yield from connection.open_channel()

        # Create a queue and an exchange on the broker
        exchange = yield from channel.declare_exchange('test.exchange', 'direct')
        queue = yield from channel.declare_queue('test.queue')

        # Bind the queue to the exchange, so the queue will get messages published to the exchange
        yield from queue.bind(exchange, 'routing.key')

        # If you pass in a dict it will be automatically converted to JSON
        msg = asynqp.Message({'test_body': 'content'})
        exchange.publish(msg, 'routing.key')

        # Synchronously get a message from the queue
        received_message = yield from queue.get()
        print(received_message.json())  # get JSON from incoming messages easily

        # Acknowledge a delivered message
        received_message.ack()

        yield from connection.close()


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_and_receive())


Installation
------------

:mod:`asynqp` has no dependencies outside of the standard library. To install the package:

::

    pip install asynqp


Table of contents
-----------------
.. toctree::
   :maxdepth: 2

   reference

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

