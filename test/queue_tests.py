import asyncio
from datetime import datetime
from contextlib import suppress
import contexts
import asynqp
from asynqp import message
from asynqp import frames
from asynqp import spec
from asynqp import exceptions
from .base_contexts import OpenChannelContext, QueueContext, ExchangeContext, BoundQueueContext, ConsumerContext
from .util import testing_exception_handler


class WhenDeclaringAQueue(OpenChannelContext):
    def when_I_declare_a_queue(self):
        self.async_partial(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True, arguments={'x-expires': 300, 'x-message-ttl': 1000}))

    def it_should_send_a_QueueDeclare_method(self):
        expected_method = spec.QueueDeclare(0, 'my.nice.queue', False, True, True, True, False, {'x-expires': 300, 'x-message-ttl': 1000})
        self.server.should_have_received_method(self.channel.id, expected_method)


class WhenQueueDeclareOKArrives(OpenChannelContext):
    def given_I_declared_a_queue(self):
        self.queue_name = 'my.nice.queue'
        self.task = asyncio.ensure_future(self.channel.declare_queue(self.queue_name, durable=True, exclusive=True, auto_delete=True, arguments={'x-expires': 300, 'x-message-ttl': 1000}))
        self.tick()

    def when_QueueDeclareOK_arrives(self):
        self.server.send_method(self.channel.id, spec.QueueDeclareOK(self.queue_name, 123, 456))
        self.result = self.task.result()

    def it_should_have_the_correct_queue_name(self):
        assert self.result.name == self.queue_name

    def it_should_be_durable(self):
        assert self.result.durable

    def it_should_be_exclusive(self):
        assert self.result.exclusive

    def it_should_auto_delete(self):
        assert self.result.auto_delete


class WhenILetTheServerPickTheQueueName(OpenChannelContext):
    def given_I_declared_a_queue(self):
        self.task = asyncio.ensure_future(self.channel.declare_queue('', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.tick()

        self.queue_name = 'randomly.generated.name'

    def when_QueueDeclareOK_arrives(self):
        self.server.send_method(self.channel.id, spec.QueueDeclareOK(self.queue_name, 123, 456))
        self.result = self.task.result()

    def it_should_return_a_queue_with_the_correct_name(self):
        assert self.result.name == self.queue_name


class WhenIUseAnIllegalNameForAQueue(OpenChannelContext):
    @classmethod
    def examples_of_bad_names(cls):
        yield 'amq.begins.with.amq.'
        yield 'contains~illegal/symbols'

    def when_I_declare_the_queue(self, queue_name):
        self.task = asyncio.ensure_future(self.channel.declare_queue(queue_name, durable=True, exclusive=True, auto_delete=True),
                                          loop=self.loop)
        self.tick()

    def it_should_throw_ValueError(self):
        assert isinstance(self.task.exception(), ValueError)


class WhenBindingAQueueToAnExchange(QueueContext, ExchangeContext):
    def when_I_bind_the_queue(self):
        self.async_partial(self.queue.bind(self.exchange, 'routing.key', arguments={'x-ignore': ''}))

    def it_should_send_QueueBind(self):
        expected_method = spec.QueueBind(0, self.queue.name, self.exchange.name, 'routing.key', False, {'x-ignore': ''})
        self.server.should_have_received_method(self.channel.id, expected_method)


class WhenBindingAQueueToTheDefaultExchange(QueueContext):
    def when_I_bind_the_queue(self):
        self.task = self.async_partial(self.queue.bind('', 'routing.key', arguments={'x-ignore': ''}))

    def it_should_throw_InvalidExchangeName(self):
        assert isinstance(self.task.exception(), exceptions.InvalidExchangeName)


class WhenQueueBindOKArrives(QueueContext, ExchangeContext):
    def given_I_sent_QueueBind(self):
        self.task = asyncio.ensure_future(self.queue.bind(self.exchange, 'routing.key'))
        self.tick()

    def when_QueueBindOK_arrives(self):
        self.server.send_method(self.channel.id, spec.QueueBindOK())
        self.binding = self.task.result()

    def then_the_returned_binding_should_have_the_correct_queue(self):
        assert self.binding.queue is self.queue

    def and_the_returned_binding_should_have_the_correct_exchange(self):
        assert self.binding.exchange is self.exchange


class WhenUnbindingAQueue(BoundQueueContext):
    def when_I_unbind_the_queue(self):
        self.async_partial(self.binding.unbind(arguments={'x-ignore': ''}))

    def it_should_send_QueueUnbind(self):
        expected_method = spec.QueueUnbind(0, self.queue.name, self.exchange.name, 'routing.key', {'x-ignore': ''})
        self.server.should_have_received_method(self.channel.id, expected_method)


class WhenQueueUnbindOKArrives(BoundQueueContext):
    def given_I_unbound_the_queue(self):
        self.task = asyncio.ensure_future(self.binding.unbind())
        self.tick()

    def when_QueueUnbindOK_arrives(self):
        self.server.send_method(self.channel.id, spec.QueueUnbindOK())

    def it_should_be_ok(self):
        assert self.task.result() is None


class WhenIUnbindAQueueTwice(BoundQueueContext):
    def given_an_unbound_queue(self):
        asyncio.ensure_future(self.binding.unbind())
        self.tick()
        self.server.send_method(self.channel.id, spec.QueueUnbindOK())

    def when_I_unbind_the_queue_again(self):
        self.task = asyncio.ensure_future(self.binding.unbind())
        self.tick()

    def it_should_throw_Deleted(self):
        assert isinstance(self.task.exception(), asynqp.Deleted)


class WhenIAskForAMessage(QueueContext):
    def when_I_get_a_message(self):
        self.async_partial(self.queue.get(no_ack=False))

    def it_should_send_BasicGet(self):
        self.server.should_have_received_method(self.channel.id, spec.BasicGet(0, self.queue.name, False))


class WhenBasicGetEmptyArrives(QueueContext):
    def given_I_asked_for_a_message(self):
        self.task = asyncio.ensure_future(self.queue.get(no_ack=False))
        self.tick()

    def when_BasicGetEmpty_arrives(self):
        self.server.send_method(self.channel.id, spec.BasicGetEmpty(''))

    def it_should_return_None(self):
        assert self.task.result() is None


class WhenBasicGetOKArrives(QueueContext):
    def given_I_asked_for_a_message(self):
        self.expected_message = asynqp.Message('body', timestamp=datetime(2014, 5, 5))
        self.task = asyncio.ensure_future(self.queue.get(no_ack=False))
        self.tick()

    def when_BasicGetOK_arrives_with_content(self):
        method = spec.BasicGetOK(123, False, 'my.exchange', 'routing.key', 0)
        self.server.send_method(self.channel.id, method)

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.server.send_frame(frames.ContentHeaderFrame(self.channel.id, header))

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.server.send_frame(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()

    def it_should_return_the_expected_message(self):
        assert self.task.result() == self.expected_message

    def it_should_put_the_exchange_name_on_the_msg(self):
        assert self.task.result().exchange_name == 'my.exchange'

    def it_should_put_the_routing_key_on_the_msg(self):
        assert self.task.result().routing_key == 'routing.key'


class WhenConnectionClosedOnGet(QueueContext):
    def given_I_asked_for_a_message(self):
        self.task = asyncio.ensure_future(self.queue.get(no_ack=False))
        self.tick()

    def when_connection_is_closed(self):
        # XXX: remove if we change behaviour to not raise
        with suppress(Exception):
            self.server.protocol.connection_lost(Exception())
        self.tick()

    def it_should_raise_exception(self):
        assert self.task.exception() is not None


class WhenISubscribeToAQueue(QueueContext):
    def when_I_start_a_consumer(self):
        self.async_partial(self.queue.consume(lambda msg: None, no_local=False, no_ack=False, exclusive=False, arguments={'x-priority': 1}))

    def it_should_send_BasicConsume(self):
        self.server.should_have_received_method(self.channel.id, spec.BasicConsume(0, self.queue.name, '', False, False, False, False, {'x-priority': 1}))


class WhenConsumeOKArrives(QueueContext):
    def given_I_started_a_consumer(self):
        self.task = asyncio.ensure_future(self.queue.consume(lambda msg: None, no_local=False, no_ack=False, exclusive=False, arguments={'x-priority': 1}))
        self.tick()

    def when_BasicConsumeOK_arrives(self):
        method = spec.BasicConsumeOK('made.up.tag')
        self.server.send_method(self.channel.id, method)

    def it_should_return_a_consumer_with_the_correct_consumer_tag(self):
        assert self.task.result().tag == 'made.up.tag'


class WhenBasicDeliverArrives(ConsumerContext):
    def given_a_message(self):
        self.expected_message = asynqp.Message('body', timestamp=datetime(2014, 5, 5))

    def when_BasicDeliver_arrives_with_content(self):
        method = spec.BasicDeliver(self.consumer.tag, 123, False, 'my.exchange', 'routing.key')
        self.server.send_method(self.channel.id, method)

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.server.send_frame(frames.ContentHeaderFrame(self.channel.id, header))

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.server.send_frame(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()

    def it_should_send_the_message_to_the_callback(self):
        self.callback.assert_called_once_with(self.expected_message)


# test that the call to handler.ready() is not affected by the exception
class WhenAConsumerThrowsAnExceptionAndAnotherMessageArrives(ConsumerContext):
    def given_a_consumer_has_thrown_an_exception(self):
        self.loop.set_exception_handler(lambda l, c: None)
        self.expected_message = asynqp.Message('body', timestamp=datetime(2014, 5, 5))
        self.consumer.callback.side_effect = Exception

        self.deliver_msg()  # cause the exception to be thrown

    def when_another_message_arrives(self):
        self.deliver_msg()

    def it_should_correctly_call_the_consumer_again(self):
        assert self.callback.call_count == 2

    def deliver_msg(self):
        method = spec.BasicDeliver(self.consumer.tag, 123, False, 'my.exchange', 'routing.key')
        self.server.send_method(self.channel.id, method)

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.server.send_frame(frames.ContentHeaderFrame(self.channel.id, header))

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.server.send_frame(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()

    def cleanup_the_exception_handler(self):
        self.loop.set_exception_handler(testing_exception_handler)


class WhenICancelAConsumer(ConsumerContext):
    def when_I_cancel_the_consumer(self):
        self.async_partial(self.consumer.cancel())

    def it_should_send_a_BasicCancel_method(self):
        self.server.should_have_received_method(self.channel.id, spec.BasicCancel(self.consumer.tag, False))


class WhenCancelOKArrives(ConsumerContext):
    def given_I_cancelled_a_consumer(self):
        asyncio.ensure_future(self.consumer.cancel())
        self.tick()

    def when_BasicCancelOK_arrives(self):
        self.server.send_method(self.channel.id, spec.BasicCancelOK(self.consumer.tag))

    def it_should_be_cancelled(self):
        assert self.consumer.cancelled


class WhenCancelOKArrivesForAConsumerWithAnOnCancelMethod(QueueContext):
    def given_I_started_and_cancelled_a_consumer(self):
        self.consumer = self.ConsumerWithOnCancel()
        task = asyncio.ensure_future(self.queue.consume(self.consumer, no_local=False, no_ack=False, exclusive=False, arguments={'x-priority': 1}))
        self.tick()
        self.server.send_method(self.channel.id, spec.BasicConsumeOK('made.up.tag'))
        self.tick()
        asyncio.ensure_future(task.result().cancel())
        self.tick()

    def when_BasicCancelOK_arrives(self):
        self.server.send_method(self.channel.id, spec.BasicCancelOK('made.up.tag'))

    def it_should_call_on_cancel(self):
        assert self.consumer.on_cancel_called

    class ConsumerWithOnCancel:
        def __init__(self):
            self.on_cancel_called = False

        def __call__(self):
            pass

        def on_cancel(self):
            self.on_cancel_called = True


class WhenAConsumerWithAnOnCancelMethodIsKilledDueToAnError(QueueContext):
    def given_I_started_a_consumer(self):
        self.consumer = self.ConsumerWithOnError()
        asyncio.ensure_future(self.queue.consume(self.consumer, no_local=False, no_ack=False, exclusive=False, arguments={'x-priority': 1}))
        self.tick()
        self.server.send_method(self.channel.id, spec.BasicConsumeOK('made.up.tag'))
        self.tick()
        self.exception = Exception()

    def when_the_connection_dies(self):
        contexts.catch(self.protocol.connection_lost, self.exception)
        self.tick()

    def it_should_call_on_error(self):
        assert self.consumer.exc.original_exc is self.exception

    class ConsumerWithOnError:
        def __init__(self):
            self.exc = None

        def __call__(self):
            pass

        def on_error(self, exc):
            self.exc = exc


class WhenIPurgeAQueue(QueueContext):
    def because_I_purge_the_queue(self):
        self.async_partial(self.queue.purge())

    def it_should_send_a_QueuePurge_method(self):
        self.server.should_have_received_method(self.channel.id, spec.QueuePurge(0, self.queue.name, False))


class WhenQueuePurgeOKArrives(QueueContext):
    def given_I_called_queue_purge(self):
        self.task = asyncio.ensure_future(self.queue.purge())
        self.tick()

    def when_QueuePurgeOK_arrives(self):
        self.server.send_method(self.channel.id, spec.QueuePurgeOK(123))

    def it_should_return(self):
        self.task.result()


class WhenDeletingAQueue(QueueContext):
    def because_I_delete_the_queue(self):
        self.async_partial(self.queue.delete(if_unused=False, if_empty=False))

    def it_should_send_a_QueueDelete_method(self):
        self.server.should_have_received_method(self.channel.id, spec.QueueDelete(0, self.queue.name, False, False, False))


class WhenQueueDeleteOKArrives(QueueContext):
    def given_I_deleted_a_queue(self):
        asyncio.ensure_future(self.queue.delete(if_unused=False, if_empty=False), loop=self.loop)
        self.tick()

    def when_QueueDeleteOK_arrives(self):
        method = spec.QueueDeleteOK(123)
        self.server.send_method(self.channel.id, method)

    def it_should_be_deleted(self):
        assert self.queue.deleted


class WhenITryToUseADeletedQueue(QueueContext):
    def given_a_deleted_queue(self):
        asyncio.ensure_future(self.queue.delete(if_unused=False, if_empty=False), loop=self.loop)
        self.tick()
        self.server.send_method(self.channel.id, spec.QueueDeleteOK(123))

    def when_I_try_to_use_the_queue(self):
        self.task = asyncio.ensure_future(self.queue.get())
        self.tick()

    def it_should_throw_Deleted(self):
        assert isinstance(self.task.exception(), asynqp.Deleted)


class WhenAConnectionIsClosedCancelConsuming(QueueContext, ExchangeContext):
    def given_a_consumer(self):
        task = asyncio.ensure_future(self.queue.consume(
            lambda x: None, no_local=False, no_ack=False,
            exclusive=False, arguments={'x-priority': 1}))
        self.tick()
        self.server.send_method(self.channel.id, spec.BasicConsumeOK('made.up.tag'))
        self.tick()
        self.consumer = task.result()

    def when_connection_is_closed(self):
        with suppress(Exception):
            self.connection.protocol.connection_lost(Exception())

    def it_should_not_hang(self):
        self.loop.run_until_complete(asyncio.wait_for(self.consumer.cancel(), 0.2))


class WhenIDeclareQueueWithPassiveAndOKArrives(OpenChannelContext):
    def given_I_declared_a_queue_with_passive(self):
        self.task = asyncio.ensure_future(self.channel.declare_queue(
            '123', durable=True, exclusive=True, auto_delete=False,
            passive=True), loop=self.loop)
        self.tick()

    def when_QueueDeclareOK_arrives(self):
        self.server.send_method(
            self.channel.id, spec.QueueDeclareOK('123', 123, 456))

    def it_should_return_queue_object(self):
        result = self.task.result()
        assert result
        assert result.name == '123'

    def it_should_have_sent_passive_in_frame(self):
        self.server.should_have_received_method(
            self.channel.id, spec.QueueDeclare(
                0, '123', True, True, True, False, False, {}))


class WhenIDeclareQueueWithPassiveAndErrorArrives(OpenChannelContext):
    def given_I_declared_a_queue_with_passive(self):
        self.task = asyncio.ensure_future(self.channel.declare_queue(
            '123', durable=True, exclusive=True, auto_delete=True,
            passive=True), loop=self.loop)
        self.tick()

    def when_error_arrives(self):
        self.server.send_method(
            self.channel.id, spec.ChannelClose(404, 'Bad queue', 40, 50))

    def it_should_raise_exception(self):
        assert isinstance(self.task.exception(), exceptions.NotFound)


class WhenIDeclareQueueWithNoWait(OpenChannelContext):
    def given_I_declared_a_queue_with_passive(self):
        self.task = asyncio.ensure_future(self.channel.declare_queue(
            '123', durable=True, exclusive=True, auto_delete=False,
            nowait=True), loop=self.loop)
        self.tick()

    def it_should_return_queue_object_without_wait(self):
        result = self.task.result()
        assert result
        assert result.name == '123'

    def it_should_have_sent_nowait_in_frame(self):
        self.server.should_have_received_method(
            self.channel.id, spec.QueueDeclare(
                0, '123', False, True, True, False, True, {}))


class WhenConsumerIsClosedServerSide(QueueContext, ExchangeContext):
    def given_a_consumer(self):
        task = asyncio.ensure_future(self.queue.consume(lambda x: None))
        self.tick()
        self.server.send_method(self.channel.id, spec.BasicConsumeOK('made.up.tag'))
        self.tick()
        self.consumer = task.result()

    def when_consumer_is_closed_server_side(self):
        self.server.send_method(self.channel.id, spec.BasicCancel('made.up.tag', False))
        self.tick()

    def it_should_close_consumer(self):
        assert 'made.up.tag' not in self.channel.queue_factory.consumers.consumers
