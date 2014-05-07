import asyncio
from datetime import datetime
from unittest import mock
import asynqp
from asynqp import frames
from asynqp import spec
from .base_contexts import OpenChannelContext, QueueContext, ExchangeContext


class WhenDeclaringAQueue(OpenChannelContext):
    def when_I_declare_a_queue(self):
        asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.go()

    def it_should_send_a_QueueDeclare_method(self):
        expected_method = spec.QueueDeclare(0, 'my.nice.queue', False, True, True, True, False, {})
        self.protocol.send_method.assert_called_once_with(self.channel.id, expected_method)


class WhenQueueDeclareOKArrives(OpenChannelContext):
    def given_I_declared_a_queue(self):
        self.queue_name = 'my.nice.queue'
        self.task = asyncio.async(self.channel.declare_queue(self.queue_name, durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.go()

    def when_QueueDeclareOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueDeclareOK(self.queue_name, 123, 456)))
        self.go()
        self.result = self.task.result()

    def it_should_have_the_correct_queue_name(self):
        assert self.result.name == self.queue_name

    def it_should_be_durable(self):
        assert self.result.durable

    def it_should_be_exclusive(self):
        assert self.result.exclusive

    def it_should_auto_delete(self):
        assert self.result.auto_delete


class WhenIDeclareTwoQueuesConcurrently(OpenChannelContext):
    def given_I_am_awaiting_QueueDeclareOK(self):
        self.queue_name1 = 'my.nice.queue'
        self.task1 = asyncio.async(self.channel.declare_queue(self.queue_name1, durable=True, exclusive=True, auto_delete=True),
                                   loop=self.loop)
        self.go()
        self.protocol.reset_mock()

    def when_I_declare_another_queue(self):
        asyncio.async(self.channel.declare_queue('another.queue', durable=True, exclusive=True, auto_delete=True),
                      loop=self.loop)
        self.go()

    def it_should_not_send_a_second_QueueDeclare_method(self):
        assert not self.protocol.method_calls


class WhenILetTheServerPickTheQueueName(OpenChannelContext):
    def given_I_declared_a_queue(self):
        self.task = asyncio.async(self.channel.declare_queue('', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.go()

        self.queue_name = 'randomly.generated.name'

    def when_QueueDeclareOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueDeclareOK(self.queue_name, 123, 456)))
        self.go()
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
        self.go()

    def it_should_throw_ValueError(self):
        assert isinstance(self.task.exception(), ValueError)


class WhenBindingAQueueToAnExchange(QueueContext, ExchangeContext):
    def when_I_bind_the_queue(self):
        asyncio.async(self.queue.bind(self.exchange, 'routing.key'))
        self.go()

    def it_should_send_QueueBind(self):
        expected_method = spec.QueueBind(0, self.queue.name, self.exchange.name, 'routing.key', False, {})
        self.protocol.send_method.assert_called_once_with(self.channel.id, expected_method)


class WhenQueueBindOKArrives(QueueContext, ExchangeContext):
    def given_I_sent_QueueBind(self):
        self.task = asyncio.async(self.queue.bind(self.exchange, 'routing.key'))
        self.go()

    def when_QueueBindOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueBindOK()))
        self.go()
        self.binding = self.task.result()

    def then_the_returned_binding_should_have_the_correct_queue(self):
        assert self.binding.queue is self.queue

    def and_the_returned_binding_should_have_the_correct_exchange(self):
        assert self.binding.exchange is self.exchange


class WhenIAskForAMessage(QueueContext):
    def when_I_get_a_message(self):
        asyncio.async(self.queue.get(no_ack=False))
        self.go()

    def it_should_send_BasicGet(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.BasicGet(0, self.queue.name, False))


class WhenBasicGetEmptyArrives(QueueContext):
    def given_I_asked_for_a_message(self):
        self.task = asyncio.async(self.queue.get(no_ack=False))
        self.go()

    def when_BasicGetEmpty_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.BasicGetEmpty('')))
        self.go()

    def it_should_return_None(self):
        assert self.task.result() is None


class WhenBasicGetOKArrives(QueueContext):
    def given_I_asked_for_a_message(self):
        self.expected_message = asynqp.Message('body', timestamp=datetime(2014, 5, 5))
        self.task = asyncio.async(self.queue.get(no_ack=False))
        self.go()

    def when_BasicGetOK_arrives_with_content(self):
        method = spec.BasicGetOK(123, False, 'my.exchange', 'routing.key', 0)
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.go()

        header = self.expected_message.header_payload(spec.BasicGet.method_type[0])
        self.dispatcher.dispatch(frames.ContentHeaderFrame(self.channel.id, header))
        self.go()

        body = self.expected_message.frame_payloads(100)[0]
        self.dispatcher.dispatch(frames.ContentBodyFrame(self.channel.id, body))
        self.go()

    def it_should_return_the_expected_message(self):
        assert self.task.result() == self.expected_message


class WhenISubscribeToAQueue(QueueContext):
    def when_I_start_a_consumer(self):
        asyncio.async(self.queue.consume(lambda msg: None, no_local=False, no_ack=False, exclusive=False))
        self.go()

    def it_should_send_BasicConsume(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.BasicConsume(0, self.queue.name, '', False, False, False, False, {}))


class WhenConsumeOKArrives(QueueContext):
    def given_I_started_a_consumer(self):
        self.task = asyncio.async(self.queue.consume(lambda msg: None, no_local=False, no_ack=False, exclusive=False))
        self.go()

    def when_BasicConsumeOK_arrives(self):
        method = spec.BasicConsumeOK('made.up.tag')
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.go()

    def it_should_return_a_consumer_with_the_correct_consumer_tag(self):
        assert self.task.result().tag == 'made.up.tag'


class WhenBasicDeliverArrives(QueueContext):
    def given_a_consumer(self):
        self.expected_message = asynqp.Message('body', timestamp=datetime(2014, 5, 5))
        self.callback = mock.Mock()

        task = asyncio.async(self.queue.consume(self.callback, no_local=False, no_ack=False, exclusive=False))
        self.go()
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.BasicConsumeOK('made.up.tag')))
        self.go()
        self.consumer = task.result()

    def when_BasicDeliver_arrives_with_content(self):
        method = spec.BasicDeliver(self.consumer.tag, 123, False, 'my.exchange', 'routing.key')
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.go()

        header = self.expected_message.header_payload(spec.BasicDeliver.method_type[0])
        self.dispatcher.dispatch(frames.ContentHeaderFrame(self.channel.id, header))
        self.go()

        body = self.expected_message.frame_payloads(100)[0]
        self.dispatcher.dispatch(frames.ContentBodyFrame(self.channel.id, body))
        self.go()

    def it_should_send_the_message_to_the_callback(self):
        self.callback.assert_called_once_with(self.expected_message)


class WhenDeletingAQueue(QueueContext):
    def because_I_delete_the_queue(self):
        asyncio.async(self.queue.delete(if_unused=False, if_empty=False))
        self.go()

    def it_should_send_a_QueueDelete_method(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.QueueDelete(0, self.queue.name, False, False, False))


class WhenQueueDeleteOKArrives(QueueContext):
    def given_I_deleted_a_queue(self):
        asyncio.async(self.queue.delete(if_unused=False, if_empty=False), loop=self.loop)
        self.go()

    def when_QueueDeleteOK_arrives(self):
        method = spec.QueueDeleteOK(123)
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.go()

    def it_should_be_deleted(self):
        assert self.queue.deleted
