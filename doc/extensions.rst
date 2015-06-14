.. _extensions:

Protocol extensions
===================

RabbitMQ, and other brokers, support certain extensions to the AMQP protocol.
`asynqp`'s support for such extensions currently includes
*optional extra arguments* to certain methods such as :meth:`Channel.declare_queue() <asynqp.Channel.declare_queue>`.

The acceptable parameters for optional argument dictionaries is implementation-dependent.
See`RabbitMQ's supported extensions <http://www.rabbitmq.com/extensions.html>`.
