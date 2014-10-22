Reference guide
===============

.. module:: asynqp

Connecting to the AMQP broker
-----------------------------

.. autofunction:: connect

.. autofunction:: connect_and_open_channel


Managing Connections and Channels
---------------------------------

Connections
~~~~~~~~~~~

.. autoclass:: Connection
    :members:


Channels
~~~~~~~~

.. autoclass:: Channel
    :members:


Sending and receiving messages with Queues and Exchanges
--------------------------------------------------------

Queues
~~~~~~

.. autoclass:: Queue
    :members:


Exchanges
~~~~~~~~~

.. autoclass:: Exchange
    :members:


Bindings
~~~~~~~~

.. autoclass:: QueueBinding
    :members:

Consumers
~~~~~~~~~

.. autoclass:: Consumer
	:members:


Message objects
---------------

.. autoclass:: Message
    :members:

.. autoclass:: asynqp.message.IncomingMessage
    :members:
