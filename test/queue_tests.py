import asyncio
from asyncio import test_utils
from asynqp import frames
from asynqp import spec
from .base_contexts import OpenChannelContext


class WhenDeclaringAQueue(OpenChannelContext):
    def when_I_declare_a_queue(self):
        asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        test_utils.run_briefly(self.loop)

    def it_should_send_a_QueueDeclare_method(self):
        expected_method = spec.QueueDeclare(0, 'my.nice.queue', False, True, True, True, False, {})
        self.protocol.send_method.assert_called_once_with(self.channel.channel_id, expected_method)


class WhenQueueDeclareOKArrives(OpenChannelContext):
    def given_I_declared_a_queue(self):
        self.queue_name = 'my.nice.queue'
        self.task = asyncio.async(self.channel.declare_queue(self.queue_name, durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        test_utils.run_briefly(self.loop)

    def when_QueueDeclareOK_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.channel_id, spec.QueueDeclareOK(self.queue_name, 123, 456)))
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


class WhenIDeclareTwoQueues(OpenChannelContext):
    def given_a_queue(self):
        queue_name = 'my.nice.queue'
        asyncio.async(self.channel.declare_queue(queue_name, durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        test_utils.run_briefly(self.loop)
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.channel_id, spec.QueueDeclareOK(queue_name, 123, 456)))
        test_utils.run_briefly(self.loop)

    def when_I_declare_another_queue(self):
        self.expected_queue_name = 'another.queue'
        task = asyncio.async(self.channel.declare_queue(self.expected_queue_name, durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        test_utils.run_briefly(self.loop)
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.channel_id, spec.QueueDeclareOK(self.expected_queue_name, 123, 456)))
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
        frame1 = frames.MethodFrame(self.channel.channel_id, spec.QueueDeclareOK(self.queue_name1, 123, 456))
        frame2 = frames.MethodFrame(self.channel.channel_id, spec.QueueDeclareOK(self.queue_name2, 123, 456))
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
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.channel_id, spec.QueueDeclareOK(self.queue_name, 123, 456)))
        test_utils.run_briefly(self.loop)
        self.result = self.task.result()

    def it_should_return_a_queue_with_the_correct_name(self):
        assert self.result.name == self.queue_name
