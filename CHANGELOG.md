What's new in `asynqp`
======================

v0.4
----

* Improved error handling.
  * When the connection to the server is lost, any futures awaiting communication
    from the server will now be cancelled.
    (Thanks to @lenzenmi, in pull request #19)
  * More detailed exceptions on channel closure.
* Support for custom RabbitMQ extensions by an `arguments` keyword parameter for a number of methods.
  (Thanks to @fweisel, in pull request #27)
* Improved compatibility with RabbitMQ's implementation of the
  wire protocol, including better support for tables.
  (Thanks to @fweisel, in pull requests #24, #25, #26, #28)
* Support for a `sock` keyword argument to `asynqp.connect`.
  (Thanks to @urbaniak, in pull request #14)

