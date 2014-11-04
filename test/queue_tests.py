import asyncio
from datetime import datetime
from unittest import mock
import asynqp
from asynqp import message
from asynqp import frames
from asynqp import spec
from .base_contexts import OpenChannelContext, QueueContext, ExchangeContext, BoundQueueContext, ConsumerContext


class WhenDeclaringAQueue(OpenChannelContext):
    def when_I_declare_a_queue(self):
        asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.tick()

    def it_should_send_a_QueueDeclare_method(self):
        expected_method = spec.QueueDeclare(0, 'my.nice.queue', False, True, True, True, False, {})
        self.protocol.send_method.assert_called_once_with(self.channel.id, expected_method)


class WhenQueueDeclareOKArrives(OpenChannelContext):
    def given_I_declared_a_queue(self):
        self.queue_name = 'my.nice.queue'
        self.task = asyncio.async(self.channel.declare_queue(self.queue_name, durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.tick()

    def when_QueueDeclareOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueDeclareOK(self.queue_name, 123, 456)))
        self.tick()
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
        self.task = asyncio.async(self.channel.declare_queue('', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.tick()

        self.queue_name = 'randomly.generated.name'

    def when_QueueDeclareOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueDeclareOK(self.queue_name, 123, 456)))
        self.tick()
        self.result = self.task.result()

    def it_should_return_a_queue_with_the_correct_name(self):
        assert self.result.name == self.queue_name


class WhenIUseAnIllegalNameForAQueue(OpenChannelContext):
    @classmethod
    def examples_of_bad_names(cls):
        yield 'amq.begins.with.amq.'
        yield 'contains~illegal/symbols'

    def when_I_declare_the_queue(self, queue_name):
        self.task = asyncio.async(self.channel.declare_queue(queue_name, durable=True, exclusive=True, auto_delete=True),
                                  loop=self.loop)
        self.tick()

    def it_should_throw_ValueError(self):
        assert isinstance(self.task.exception(), ValueError)


class WhenBindingAQueueToAnExchange(QueueContext, ExchangeContext):
    def when_I_bind_the_queue(self):
        asyncio.async(self.queue.bind(self.exchange, 'routing.key'))
        self.tick()

    def it_should_send_QueueBind(self):
        expected_method = spec.QueueBind(0, self.queue.name, self.exchange.name, 'routing.key', False, {})
        self.protocol.send_method.assert_called_once_with(self.channel.id, expected_method)


class WhenQueueBindOKArrives(QueueContext, ExchangeContext):
    def given_I_sent_QueueBind(self):
        self.task = asyncio.async(self.queue.bind(self.exchange, 'routing.key'))
        self.tick()

    def when_QueueBindOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueBindOK()))
        self.tick()
        self.binding = self.task.result()

    def then_the_returned_binding_should_have_the_correct_queue(self):
        assert self.binding.queue is self.queue

    def and_the_returned_binding_should_have_the_correct_exchange(self):
        assert self.binding.exchange is self.exchange


class WhenUnbindingAQueue(BoundQueueContext):
    def when_I_unbind_the_queue(self):
        asyncio.async(self.binding.unbind())
        self.tick()

    def it_should_send_QueueUnbind(self):
        expected_method = spec.QueueUnbind(0, self.queue.name, self.exchange.name, 'routing.key', {})
        self.protocol.send_method.assert_called_once_with(self.channel.id, expected_method)


class WhenQueueUnbindOKArrives(BoundQueueContext):
    def given_I_unbound_the_queue(self):
        self.task = asyncio.async(self.binding.unbind())
        self.tick()

    def when_QueueUnbindOK_arrives(self):
        frame = frames.Frame(self.channel.id, spec.QueueUnbindOK())
        self.dispatcher.dispatch(frame)
        self.tick()

    def it_should_be_ok(self):
        assert self.task.result() is None


class WhenIUnbindAQueueTwice(BoundQueueContext):
    def given_an_unbound_queue(self):
        asyncio.async(self.binding.unbind())
        self.tick()
        self.dispatcher.dispatch(frames.Frame(self.channel.id, spec.QueueUnbindOK()))
        self.tick()

    def when_I_unbind_the_queue_again(self):
        self.task = asyncio.async(self.binding.unbind())
        self.tick()

    def it_should_throw_Deleted(self):
        assert isinstance(self.task.exception(), asynqp.Deleted)


class WhenIAskForAMessage(QueueContext):
    def when_I_get_a_message(self):
        asyncio.async(self.queue.get(no_ack=False))
        self.tick()

    def it_should_send_BasicGet(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.BasicGet(0, self.queue.name, False))


class WhenBasicGetEmptyArrives(QueueContext):
    def given_I_asked_for_a_message(self):
        self.task = asyncio.async(self.queue.get(no_ack=False))
        self.tick()

    def when_BasicGetEmpty_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.BasicGetEmpty('')))
        self.tick()

    def it_should_return_None(self):
        assert self.task.result() is None


class WhenBasicGetOKArrives(QueueContext):
    def given_I_asked_for_a_message(self):
        self.expected_message = asynqp.Message('body', timestamp=datetime(2014, 5, 5))
        self.task = asyncio.async(self.queue.get(no_ack=False))
        self.tick()

    def when_BasicGetOK_arrives_with_content(self):
        method = spec.BasicGetOK(123, False, 'my.exchange', 'routing.key', 0)
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.tick()

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.dispatcher.dispatch(frames.ContentHeaderFrame(self.channel.id, header))
        self.tick()

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.dispatcher.dispatch(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()
        self.tick()

    def it_should_return_the_expected_message(self):
        assert self.task.result() == self.expected_message

    def it_should_put_the_exchange_name_on_the_msg(self):
        assert self.task.result().exchange_name == 'my.exchange'

    def it_should_put_the_routing_key_on_the_msg(self):
        assert self.task.result().routing_key == 'routing.key'


class WhenISubscribeToAQueue(QueueContext):
    def when_I_start_a_consumer(self):
        asyncio.async(self.queue.consume(lambda msg: None, no_local=False, no_ack=False, exclusive=False))
        self.tick()

    def it_should_send_BasicConsume(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.BasicConsume(0, self.queue.name, '', False, False, False, False, {}))


class WhenConsumeOKArrives(QueueContext):
    def given_I_started_a_consumer(self):
        self.task = asyncio.async(self.queue.consume(lambda msg: None, no_local=False, no_ack=False, exclusive=False))
        self.tick()

    def when_BasicConsumeOK_arrives(self):
        method = spec.BasicConsumeOK('made.up.tag')
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.tick()

    def it_should_return_a_consumer_with_the_correct_consumer_tag(self):
        assert self.task.result().tag == 'made.up.tag'


class WhenBasicDeliverArrives(ConsumerContext):
    def given_a_message(self):
        self.expected_message = asynqp.Message('body', timestamp=datetime(2014, 5, 5))

    def when_BasicDeliver_arrives_with_content(self):
        method = spec.BasicDeliver(self.consumer.tag, 123, False, 'my.exchange', 'routing.key')
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.tick()

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.dispatcher.dispatch(frames.ContentHeaderFrame(self.channel.id, header))
        self.tick()

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.dispatcher.dispatch(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()
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
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.tick()

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.dispatcher.dispatch(frames.ContentHeaderFrame(self.channel.id, header))
        self.tick()

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.dispatcher.dispatch(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()
        self.tick()

    def cleanup_the_exception_handler(self):
        self.loop.set_exception_handler(None)


class WhenICancelAConsumer(ConsumerContext):
    def when_I_cancel_the_consumer(self):
        asyncio.async(self.consumer.cancel())
        self.tick()

    def it_should_send_a_BasicCancel_method(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.BasicCancel(self.consumer.tag, False))


class WhenCancelOKArrives(ConsumerContext):
    def given_I_cancelled_a_consumer(self):
        asyncio.async(self.consumer.cancel())
        self.tick()

    def when_BasicCancelOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.BasicCancelOK(self.consumer.tag)))
        self.tick()

    def it_should_be_cancelled(self):
        assert self.consumer.cancelled


class WhenIPurgeAQueue(QueueContext):
    def because_I_purge_the_queue(self):
        asyncio.async(self.queue.purge())
        self.tick()

    def it_should_send_a_QueuePurge_method(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.QueuePurge(0, self.queue.name, False))


class WhenQueuePurgeOKArrives(QueueContext):
    def given_I_called_queue_purge(self):
        self.task = asyncio.async(self.queue.purge())
        self.tick()

    def when_QueuePurgeOK_arrives(self):
        frame = frames.MethodFrame(self.channel.id, spec.QueuePurgeOK(123))
        self.dispatcher.dispatch(frame)
        self.tick()

    def it_should_return(self):
        self.task.result()


class WhenDeletingAQueue(QueueContext):
    def because_I_delete_the_queue(self):
        asyncio.async(self.queue.delete(if_unused=False, if_empty=False))
        self.tick()

    def it_should_send_a_QueueDelete_method(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.QueueDelete(0, self.queue.name, False, False, False))


class WhenQueueDeleteOKArrives(QueueContext):
    def given_I_deleted_a_queue(self):
        asyncio.async(self.queue.delete(if_unused=False, if_empty=False), loop=self.loop)
        self.tick()

    def when_QueueDeleteOK_arrives(self):
        method = spec.QueueDeleteOK(123)
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.tick()

    def it_should_be_deleted(self):
        assert self.queue.deleted


class WhenITryToUseADeletedQueue(QueueContext):
    def given_a_deleted_queue(self):
        asyncio.async(self.queue.delete(if_unused=False, if_empty=False), loop=self.loop)
        self.tick()
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueDeleteOK(123)))
        self.tick()

    def when_I_try_to_use_the_queue(self):
        self.task = asyncio.async(self.queue.get())
        self.tick()

    def it_should_throw_Deleted(self):
        assert isinstance(self.task.exception(), asynqp.Deleted)
