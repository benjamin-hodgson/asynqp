import asyncio
from asyncio import test_utils
from datetime import datetime
import asynqp
from asynqp import frames
from asynqp import spec
from .base_contexts import OpenChannelContext, QueueContext, ExchangeContext


class WhenDeclaringAQueue(OpenChannelContext):
    def when_I_declare_a_queue(self):
        asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        test_utils.run_briefly(self.loop)

    def it_should_send_a_QueueDeclare_method(self):
        expected_method = spec.QueueDeclare(0, 'my.nice.queue', False, True, True, True, False, {})
        self.protocol.send_method.assert_called_once_with(self.channel.id, expected_method)


class WhenQueueDeclareOKArrives(OpenChannelContext):
    def given_I_declared_a_queue(self):
        self.queue_name = 'my.nice.queue'
        self.task = asyncio.async(self.channel.declare_queue(self.queue_name, durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        test_utils.run_briefly(self.loop)

    def when_QueueDeclareOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueDeclareOK(self.queue_name, 123, 456)))
        test_utils.run_briefly(self.loop)
        self.result = self.task.result()

    def it_should_have_the_correct_queue_name(self):
        assert self.result.name == self.queue_name

    def it_should_be_durable(self):
        assert self.result.durable

    def it_should_be_exclusive(self):
        assert self.result.exclusive

    def it_should_auto_delete(self):
        assert self.result.auto_delete


class WhenIDeclareTwoQueues(QueueContext):
    def when_I_declare_another_queue(self):
        self.expected_queue_name = 'another.queue'
        task = asyncio.async(self.channel.declare_queue(self.expected_queue_name, durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        test_utils.run_briefly(self.loop)
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueDeclareOK(self.expected_queue_name, 123, 456)))
        test_utils.run_briefly(self.loop)
        self.result = task.result()

    def it_should_have_the_correct_queue_name(self):
        assert self.result.name == self.expected_queue_name


class WhenIDeclareTwoQueuesConcurrently(OpenChannelContext):
    def given_I_declared_two_queues(self):
        self.queue_name1 = 'my.nice.queue'
        self.task1 = asyncio.async(self.channel.declare_queue(self.queue_name1, durable=True, exclusive=True, auto_delete=True),
                                   loop=self.loop)

        self.queue_name2 = 'another.queue'
        self.task2 = asyncio.async(self.channel.declare_queue(self.queue_name2, durable=True, exclusive=True, auto_delete=True),
                                   loop=self.loop)

        test_utils.run_briefly(self.loop)

    def because_both_QueueDeclareOK_frames_arrive(self):
        frame1 = frames.MethodFrame(self.channel.id, spec.QueueDeclareOK(self.queue_name1, 123, 456))
        frame2 = frames.MethodFrame(self.channel.id, spec.QueueDeclareOK(self.queue_name2, 123, 456))
        self.dispatcher.dispatch(frame1)
        self.dispatcher.dispatch(frame2)

        test_utils.run_briefly(self.loop)

    def it_should_return_the_first_queue(self):
        assert self.task1.result().name == self.queue_name1

    def it_should_return_the_second_queue(self):
        assert self.task2.result().name == self.queue_name2


class WhenILetTheServerPickTheQueueName(OpenChannelContext):
    def given_I_declared_a_queue(self):
        self.task = asyncio.async(self.channel.declare_queue('', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        test_utils.run_briefly(self.loop)

        self.queue_name = 'randomly.generated.name'

    def when_QueueDeclareOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueDeclareOK(self.queue_name, 123, 456)))
        test_utils.run_briefly(self.loop)
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
        test_utils.run_briefly(self.loop)

    def it_should_throw_ValueError(self):
        assert isinstance(self.task.exception(), ValueError)


class WhenBindingAQueueToAnExchange(QueueContext, ExchangeContext):
    def when_I_bind_the_queue(self):
        asyncio.async(self.queue.bind(self.exchange, 'routing.key'))
        test_utils.run_briefly(self.loop)

    def it_should_send_QueueBind(self):
        expected_method = spec.QueueBind(0, self.queue.name, self.exchange.name, 'routing.key', False, {})
        self.protocol.send_method.assert_called_once_with(self.channel.id, expected_method)


class WhenQueueBindOKArrives(QueueContext, ExchangeContext):
    def given_I_sent_QueueBind(self):
        self.task = asyncio.async(self.queue.bind(self.exchange, 'routing.key'))
        test_utils.run_briefly(self.loop)

    def when_QueueBindOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.QueueBindOK()))
        test_utils.run_briefly(self.loop)
        self.binding = self.task.result()

    def then_the_returned_binding_should_have_the_correct_queue(self):
        assert self.binding.queue is self.queue

    def and_the_returned_binding_should_have_the_correct_exchange(self):
        assert self.binding.exchange is self.exchange


class WhenIAskForAMessage(QueueContext):
    def when_I_get_a_message(self):
        asyncio.async(self.queue.get(no_ack=False))
        test_utils.run_briefly(self.loop)

    def it_should_send_BasicGet(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.BasicGet(0, self.queue.name, False))


class WhenBasicGetEmptyArrives(QueueContext):
    def given_I_asked_for_a_message(self):
        self.task = asyncio.async(self.queue.get(no_ack=False))
        test_utils.run_briefly(self.loop)

    def when_BasicGetEmpty_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.BasicGetEmpty('')))
        test_utils.run_briefly(self.loop)

    def it_should_return_None(self):
        assert self.task.result() is None


class WhenBasicGetOKArrives(QueueContext):
    def given_I_asked_for_a_message(self):
        self.expected_message = asynqp.Message('body', timestamp=datetime(2014, 5, 5))
        self.task = asyncio.async(self.queue.get(no_ack=False))
        test_utils.run_briefly(self.loop)

    def when_BasicGetOK_arrives_with_content(self):
        method = spec.BasicGetOK(123, False, 'my.exchange', 'routing.key', 0)
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        test_utils.run_briefly(self.loop)

        header = self.expected_message.header_payload(spec.BasicGet.method_type[0])
        self.dispatcher.dispatch(frames.ContentHeaderFrame(self.channel.id, header))
        test_utils.run_briefly(self.loop)

        body = self.expected_message.frame_payloads(100)[0]
        self.dispatcher.dispatch(frames.ContentBodyFrame(self.channel.id, body))
        test_utils.run_briefly(self.loop)

    def it_should_return_the_expected_message(self):
        assert self.task.result() == self.expected_message


class WhenDeletingAQueue(QueueContext):
    def because_I_delete_the_queue(self):
        asyncio.async(self.queue.delete(if_unused=False, if_empty=False))
        test_utils.run_briefly(self.loop)

    def it_should_send_a_QueueDelete_method(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.QueueDelete(0, self.queue.name, False, False, False))


class WhenQueueDeleteOKArrives(QueueContext):
    def given_I_deleted_a_queue(self):
        asyncio.async(self.queue.delete(if_unused=False, if_empty=False), loop=self.loop)
        test_utils.run_briefly(self.loop)

    def when_QueueDeleteOK_arrives(self):
        method = spec.QueueDeleteOK(123)
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        test_utils.run_briefly(self.loop)

    def it_should_be_deleted(self):
        assert self.queue.deleted
