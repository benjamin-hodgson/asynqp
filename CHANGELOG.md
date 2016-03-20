What's new in `asynqp`
======================


v0.5
----
* Channels will no longer break if their calls are cancelled. Issue #52.
* Fixed message properties decoding without `content_type`. Pull request #66.
* Added `nowait` argument to Exchange and Queue declarations.
* Added `passive` argument to Exchange and Queue declarations.
* Added `on_error` and `on_cancel` callbacks for Consumer. Issue #34
* Changed the closing scheme for Channel/Connection. Proper exceptions are now
always propagated to user. Issues #57, #58
* Complete internals refactor and cleanup. Rull requests #48, #49, #50. 
* Add NO_DELAY option for socket. Issue #40. (Thanks to @socketpair for PR #41)
* Change heartbeat to be a proper background task. Issue #45. 
* `loop` is now proparly passed to all components from open_connection call. Pull request #42.
* Add support for Python up to 3.5.

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

