import asyncio
import uuid
from datetime import datetime
from unittest import mock
import asynqp
from asynqp import spec
from asynqp import frames
from asynqp import message
from .base_contexts import OpenChannelContext, ExchangeContext


class WhenDeclaringAnExchange(OpenChannelContext):
    def when_I_declare_an_exchange(self):
        asyncio.async(self.channel.declare_exchange('my.nice.exchange', 'fanout', durable=True, auto_delete=False, internal=False),
                      loop=self.loop)
        self.tick()

    def it_should_send_ExchangeDeclare(self):
        expected_method = spec.ExchangeDeclare(0, 'my.nice.exchange', 'fanout', False, True, False, False, False, {})
        self.protocol.send_method.assert_called_once_with(self.channel.id, expected_method)


class WhenExchangeDeclareOKArrives(OpenChannelContext):
    def given_I_declared_an_exchange(self):
        self.task = asyncio.async(self.channel.declare_exchange('my.nice.exchange', 'fanout', durable=True, auto_delete=False, internal=False),
                                  loop=self.loop)
        self.tick()

    def when_the_reply_arrives(self):
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, spec.ExchangeDeclareOK()))
        self.tick()
        self.result = self.task.result()

    def it_should_have_the_correct_name(self):
        assert self.result.name == 'my.nice.exchange'

    def it_should_have_the_correct_type(self):
        assert self.result.type == 'fanout'

    def it_should_be_durable(self):
        assert self.result.durable

    def it_should_not_auto_delete(self):
        assert not self.result.auto_delete

    def it_should_not_be_internal(self):
        assert not self.result.internal


# "The server MUST pre-declare a direct exchange with no public name
# to act as the default exchange for content Publish methods and for default queue bindings."
# Clients are not allowed to re-declare the default exchange, but they are allowed to publish to it
class WhenIDeclareTheDefaultExchange(OpenChannelContext):
    def when_I_declare_an_exchange_with_an_empty_name(self):
        task = asyncio.async(self.channel.declare_exchange('', 'direct', durable=True, auto_delete=False, internal=False),
                             loop=self.loop)
        self.tick()
        self.exchange = task.result()

    def it_should_not_send_exchange_declare(self):
        assert not self.protocol.send_method.called

    def it_should_return_an_exchange_with_no_name(self):
        assert self.exchange.name == ''

    def it_should_be_a_direct_exchange(self):
        assert self.exchange.type == 'direct'

    def it_should_be_durable(self):
        assert self.exchange.durable

    def it_should_not_auto_delete(self):
        assert not self.exchange.auto_delete

    def it_should_not_be_internal(self):
        assert not self.exchange.internal


class WhenIUseAnIllegalExchangeName(OpenChannelContext):
    @classmethod
    def examples_of_bad_words(cls):
        yield "amq.starts.with.amq."
        yield "contains'illegal$ymbols"

    def because_I_try_to_declare_the_exchange(self, name):
        task = asyncio.async(self.channel.declare_exchange(name, 'direct'))
        self.tick()
        self.exception = task.exception()

    def it_should_throw_ValueError(self):
        assert isinstance(self.exception, ValueError)


class WhenPublishingAShortMessage(ExchangeContext):
    def given_a_message(self):
        self.correlation_id = str(uuid.uuid4())
        self.message_id = str(uuid.uuid4())
        self.timestamp = datetime(2014, 5, 4)
        self.msg = asynqp.Message(
            'body',
            content_type='application/json',
            content_encoding='utf-8',
            headers={},
            delivery_mode=2,
            priority=5,
            correlation_id=self.correlation_id,
            reply_to='me',
            expiration='tomorrow',
            message_id=self.message_id,
            timestamp=self.timestamp,
            type='telegram',
            user_id='benjamin',
            app_id='asynqptests'
        )

    def when_I_publish_the_message(self):
        self.exchange.publish(self.msg, 'routing.key', mandatory=True)

    def it_should_send_a_BasicPublish_method_followed_by_a_header_and_the_body(self):
        expected_method = spec.BasicPublish(0, self.exchange.name, 'routing.key', True, False)
        header_payload = message.ContentHeaderPayload(60, 4, [
            'application/json',
            'utf-8',
            {}, 2, 5,
            self.correlation_id,
            'me', 'tomorrow',
            self.message_id,
            self.timestamp,
            'telegram',
            'benjamin',
            'asynqptests'
        ])
        expected_header = frames.ContentHeaderFrame(self.channel.id, header_payload)
        expected_body = frames.ContentBodyFrame(self.channel.id, b'body')
        assert self.protocol.mock_calls == [
            mock.call.send_method(self.channel.id, expected_method),
            mock.call.send_frame(expected_header),
            mock.call.send_frame(expected_body)
        ]


class WhenPublishingALongMessage(ExchangeContext):
    def given_a_message(self):
        self.body1 = b"a" * (self.frame_max - 8)
        self.body2 = b"b" * (self.frame_max - 8)
        self.body3 = b"c" * (self.frame_max - 8)
        body = self.body1 + self.body2 + self.body3
        self.msg = asynqp.Message(body)

    def when_I_publish_the_message(self):
        self.exchange.publish(self.msg, 'routing.key')

    def it_should_send_multiple_body_frames(self):
        expected_body1 = frames.ContentBodyFrame(self.channel.id, self.body1)
        expected_body2 = frames.ContentBodyFrame(self.channel.id, self.body2)
        expected_body3 = frames.ContentBodyFrame(self.channel.id, self.body3)
        self.protocol.send_frame.assert_has_calls([
            mock.call(expected_body1),
            mock.call(expected_body2),
            mock.call(expected_body3)
        ], any_order=False)


class WhenDeletingAnExchange(ExchangeContext):
    def when_I_delete_the_exchange(self):
        asyncio.async(self.exchange.delete(if_unused=True), loop=self.loop)
        self.tick()

    def it_should_send_ExchangeDelete(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.ExchangeDelete(0, self.exchange.name, True, False))


class WhenExchangeDeleteOKArrives(ExchangeContext):
    def given_I_deleted_the_exchange(self):
        asyncio.async(self.exchange.delete(if_unused=True), loop=self.loop)
        self.tick()

    def when_confirmation_arrives(self):
        frame = frames.MethodFrame(self.channel.id, spec.ExchangeDeleteOK())
        self.dispatcher.dispatch(frame)

    def it_should_be_ok(self):
        pass
