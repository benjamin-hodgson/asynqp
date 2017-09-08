import asyncio
import json
import uuid
from datetime import datetime
import asynqp
from asynqp import amqptypes
from asynqp import message
from asynqp import spec
from asynqp import frames
from .base_contexts import QueueContext


class WhenGettingTheContentHeader:
    def given_a_message(self):
        self.correlation_id = str(uuid.uuid4())
        self.message_id = str(uuid.uuid4())
        self.timestamp = datetime.fromtimestamp(12345)
        self.message = asynqp.Message(
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

    def when_I_ask_for_the_header(self):
        self.payload = message.get_header_payload(self.message, 50)

    def it_should_return_the_frames(self):
        assert self.payload == message.ContentHeaderPayload(50, 4, [
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


class WhenIPassInADictWithNoContentHeader:
    def when_I_make_a_message_with_a_dict_and_no_content_type(self):
        self.body = {'somestuff': 123}
        self.message = asynqp.Message(self.body)

    def it_should_jsonify_the_dict(self):
        assert json.loads(self.message.body.decode(self.message.content_encoding)) == self.body

    def it_should_set_the_content_type_for_me(self):
        assert self.message.content_type == 'application/json'


class WhenIPassInADictWithAContentTypeHeader:
    def when_I_make_a_message_with_a_dict_and_a_content_type(self):
        self.body = {'somestuff': 123}
        self.message = asynqp.Message(self.body, content_type='application/vnd.my.mime.type')

    def it_should_jsonify_the_dict(self):
        assert json.loads(self.message.body.decode(self.message.content_encoding)) == self.body

    def it_should_not_set_the_content_type_for_me(self):
        assert self.message.content_type == 'application/vnd.my.mime.type'


class WhenIPassInAStrWithNoEncoding:
    def when_I_make_a_message_with_a_str(self):
        self.body = "my_str"
        self.message = asynqp.Message(self.body)

    def it_should_encode_the_body_as_utf8_for_me(self):
        assert self.message.body.decode('utf-8') == self.body


class WhenIPassInAStrWithAnEncoding:
    def when_I_make_a_message_with_a_str_and_an_encoding(self):
        self.body = "my_str"
        self.message = asynqp.Message(self.body, content_encoding='latin-1')

    def it_should_encode_the_body_with_the_encoding_I_wanted(self):
        assert self.message.body.decode('latin-1') == self.body


class WhenIPassInBytes:
    def when_I_make_a_message_with_bytes(self):
        self.body = b'hello'
        self.message = asynqp.Message(self.body)

    def it_should_not_try_to_decode_the_body(self):
        assert self.message.body == self.body


class WhenGettingFramesForAShortMessage:
    def given_a_message(self):
        self.message = asynqp.Message('body')

    def when_I_get_the_frames(self):
        self.frames = message.get_frame_payloads(self.message, 100)

    def it_should_return_one_frame(self):
        assert self.frames == [b'body']


class WhenGettingFramesForALongMessage:
    def given_a_message(self):
        self.message = asynqp.Message('much longer body')

    def because_the_message_is_longer_than_the_max_size(self):
        self.frames = message.get_frame_payloads(self.message, 5)

    def it_should_split_the_body_into_frames(self):
        assert self.frames == [b'much ', b'longe', b'r bod', b'y']


class WhenIAcknowledgeADeliveredMessage(QueueContext):
    def given_I_received_a_message(self):
        self.delivery_tag = 12487

        msg = asynqp.Message('body', timestamp=datetime(2014, 5, 5))
        task = asyncio.ensure_future(self.queue.get())
        self.tick()
        method = spec.BasicGetOK(self.delivery_tag, False, 'my.exchange', 'routing.key', 0)
        self.server.send_method(self.channel.id, method)

        header = message.get_header_payload(msg, spec.BasicGet.method_type[0])
        self.server.send_frame(frames.ContentHeaderFrame(self.channel.id, header))

        body = message.get_frame_payloads(msg, 100)[0]
        self.server.send_frame(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()

        self.msg = task.result()

    def when_I_ack_the_message(self):
        self.msg.ack()

    def it_should_send_BasicAck(self):
        self.server.should_have_received_method(self.channel.id, spec.BasicAck(self.delivery_tag, False))


class WhenIRejectADeliveredMessage(QueueContext):
    def given_I_received_a_message(self):
        self.delivery_tag = 12487

        msg = asynqp.Message('body', timestamp=datetime(2014, 5, 5))
        task = asyncio.ensure_future(self.queue.get())
        self.tick()
        method = spec.BasicGetOK(self.delivery_tag, False, 'my.exchange', 'routing.key', 0)
        self.server.send_method(self.channel.id, method)

        header = message.get_header_payload(msg, spec.BasicGet.method_type[0])
        self.server.send_frame(frames.ContentHeaderFrame(self.channel.id, header))

        body = message.get_frame_payloads(msg, 100)[0]
        self.server.send_frame(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()

        self.msg = task.result()

    def when_I_reject_the_message(self):
        self.msg.reject(requeue=True)

    def it_should_send_BasicReject(self):
        self.server.should_have_received_method(self.channel.id, spec.BasicReject(self.delivery_tag, True))


class WhenIGetJSONFromADeliveredMessage:
    def given_a_message(self):
        self.body = {'x': 123, 'y': ['json', 15, 'c00l']}
        self.msg = asynqp.Message(self.body, timestamp=datetime(2014, 5, 5))

    def when_I_get_the_json(self):
        self.result = self.msg.json()

    def it_should_give_me_the_body(self):
        assert self.result == self.body


class WhenSettingAProperty:
    def given_a_message(self):
        self.msg = asynqp.Message("abc")

    def when_I_set_a_property(self):
        self.msg.content_type = "application/json"

    def it_should_cast_it_to_the_correct_amqp_type(self):
        assert isinstance(self.msg.content_type, amqptypes.ShortStr)
        assert self.msg.content_type == amqptypes.ShortStr("application/json")


class WhenSettingAPropertyAndIHaveAlreadyCastItMyself:
    def given_a_message(self):
        self.msg = asynqp.Message("abc")
        self.val = amqptypes.ShortStr("application/json")

    def when_I_set_a_property(self):
        self.msg.content_type = self.val

    def it_should_not_attempt_to_cast_it(self):
        assert self.msg.content_type is self.val


class WhenSettingAnAttributeThatIsNotAProperty:
    def given_a_message(self):
        self.msg = asynqp.Message("abc")

    def when_I_set_a_property(self):
        self.msg.foo = 123

    def it_should_not_attempt_to_cast_it(self):
        assert self.msg.foo == 123


class WhenIReadAContentHeaderWithoutAllProperties:

    def given_headers_wit_only_content_encoding(self):
        self.data = (
            b'\x00<\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08p\x00'
            b'\x05utf-8\x00\x00\x00\x00\x01')

    def when_I_read_properties(self):
        self.payload = message.ContentHeaderPayload.read(self.data)

    def it_should_have_only_content_encoding(self):
        assert self.payload == message.ContentHeaderPayload(
            60, 8, [None, 'utf-8', {}, 1, None, None,
                    None, None, None, None, None, None, None])
