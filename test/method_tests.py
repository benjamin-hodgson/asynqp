import asynqp
from asynqp import spec
from asynqp import frames
from asynqp import amqptypes
from asynqp import message
from .base_contexts import ProtocolContext, MockDispatcherContext


class WhenConnectionStartArrives(MockDispatcherContext):
    def given_a_connection_start_method_I_copied_from_the_rabbitmq_server(self):
        self.raw = (b"\x01\x00\x00\x00\x00\x01\x50"  # type, channel, size
                    b"\x00\x0A\x00\x0A\x00\t\x00\x00\x01"
                    b"%\x0ccapabilitiesF\x00\x00\x00X\x12publisher_confirmst\x01\x1aexchange_exchange_bindings"
                    b"t\x01\nbasic.nackt\x01\x16consumer_cancel_notifyt\x01\tcopyrightS\x00\x00\x00'Copyright "
                    b"(C) 2007-2013 GoPivotal, Inc.\x0binformationS\x00\x00\x005Licensed under the MPL. "
                    b" See http://www.rabbitmq.com/\x08platformS\x00\x00\x00\nErlang/OTP\x07productS\x00\x00\x00\x08"
                    b"RabbitMQ\x07versionS\x00\x00\x00\x053.1.5"
                    b"\x00\x00\x00\x0eAMQPLAIN PLAIN\x00\x00\x00\x0Ben_US en_GB\xCE")

        expected_method = spec.ConnectionStart(0, 9, {
            'capabilities': {'publisher_confirms': True,
                             'exchange_exchange_bindings': True,
                             'basic.nack': True,
                             'consumer_cancel_notify': True},
            'copyright': 'Copyright (C) 2007-2013 GoPivotal, Inc.',
            'information': 'Licensed under the MPL.  See http://www.rabbitmq.com/',
            'platform': 'Erlang/OTP',
            'product': 'RabbitMQ',
            'version': '3.1.5'
        }, 'AMQPLAIN PLAIN', 'en_US en_GB')
        self.expected_frame = asynqp.frames.MethodFrame(0, expected_method)

    def when_the_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_dispatch_a_correctly_deserialised_ConnectionStart_method(self):
        self.dispatcher.dispatch.assert_called_once_with(self.expected_frame)


class WhenSendingConnectionStartOK(ProtocolContext):
    def given_a_method_to_send(self):
        method = spec.ConnectionStartOK({'somecrap': 'aboutme'}, 'AMQPLAIN', {'auth': 'info'}, 'en_US')
        self.frame = asynqp.frames.MethodFrame(0, method)

    def when_we_send_the_method(self):
        self.protocol.send_frame(self.frame)

    def it_should_send_the_correct_bytestring(self):
        expected_bytes = (b'\x01\x00\x00\x00\x00\x00>\x00\n\x00\x0b\x00\x00\x00\x15\x08somecrapS'
                          b'\x00\x00\x00\x07aboutme\x08AMQPLAIN\x00\x00\x00\x0e\x04'
                          b'authS\x00\x00\x00\x04info\x05en_US\xce')
        self.transport.write.assert_called_once_with(expected_bytes)


class WhenConnectionTuneArrives(MockDispatcherContext):
    def given_a_connection_tune_method_I_copied_from_the_rabbitmq_server(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x0C\x00\x0A\x00\x1E\x00\x00\x00\x02\x00\x00\x02\x58\xCE'
        expected_method = spec.ConnectionTune(0, 131072, 600)
        self.expected_frame = asynqp.frames.MethodFrame(0, expected_method)

    def when_the_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_dispatch_a_correctly_deserialised_ConnectionTune_method(self):
        self.dispatcher.dispatch.assert_called_once_with(self.expected_frame)


class WhenSendingConnectionTuneOK(ProtocolContext):
    def given_a_method_to_send(self):
        method = spec.ConnectionTuneOK(1024, 131072, 10)
        self.frame = asynqp.frames.MethodFrame(0, method)

    def when_I_send_the_method(self):
        self.protocol.send_frame(self.frame)

    def it_should_write_the_correct_bytestring(self):
        self.transport.write.assert_called_once_with(b'\x01\x00\x00\x00\x00\x00\x0C\x00\n\x00\x1F\x04\x00\x00\x02\x00\x00\x00\x0A\xCE')


class WhenSendingConnectionOpen(ProtocolContext):
    def given_a_method_to_send(self):
        method = spec.ConnectionOpen('/', '', False)
        self.frame = asynqp.frames.MethodFrame(0, method)

    def when_I_send_the_method(self):
        self.protocol.send_frame(self.frame)

    def it_should_write_the_correct_bytestring(self):
        self.transport.write.assert_called_once_with(b'\x01\x00\x00\x00\x00\x00\x08\x00\x0A\x00\x28\x01/\x00\x00\xCE')


class WhenSendingQueueDeclare(ProtocolContext):
    def given_a_method_to_send(self):
        self.method = spec.QueueDeclare(0, 'a', False, False, False, True, False, {})

    def when_I_send_the_method(self):
        self.protocol.send_method(1, self.method)

    def it_should_write_the_correct_bytestring(self):
        self.transport.write.assert_called_once_with(b'\x01\x00\x01\x00\x00\x00\x0D\x00\x32\x00\x0A\x00\x00\x01a\x08\x00\x00\x00\x00\xCE')


class WhenSendingContentHeader(ProtocolContext):
    def given_a_content_header_frame(self):
        payload = message.ContentHeaderPayload(50, 100, [amqptypes.Octet(3), None, amqptypes.Table({'some': 'value'})])
        self.frame = frames.ContentHeaderFrame(1, payload)

    def when_I_send_the_frame(self):
        self.protocol.send_frame(self.frame)

    def it_should_write_the_correct_bytestring(self):
        self.transport.write.assert_called_once_with(
            b'\x02\x00\x01\x00\x00\x00\x22'  # regular frame header
            b'\x00\x32\x00\x00'  # class id 50; weight is always 0
            b'\x00\x00\x00\x00\x00\x00\x00\x64'  # body length 100
            b'\xA0\x00'  # property_flags 0b1010000000000000
            b'\x03\x00\x00\x00\x0F\x04someS\x00\x00\x00\x05value'  # property list
            b'\xCE')


class WhenAContentHeaderArrives(MockDispatcherContext):
    def given_a_content_header_frame(self):
        self.raw = (
            b'\x02\x00\x01\x00\x00\x00\x25'  # regular frame header
            b'\x00\x32\x00\x00'  # class id 50; weight is always 0
            b'\x00\x00\x00\x00\x00\x00\x00\x64'  # body length 100
            b'\xA0\x00'  # property_flags 0b1010000000000000
            b'\x03yes\x00\x00\x00\x0F\x04someS\x00\x00\x00\x05value'  # property list
            b'\xCE')

        expected_payload = message.ContentHeaderPayload(50, 100, [
            amqptypes.ShortStr('yes'), None, amqptypes.Table({'some': 'value'}),
            None, None, None, None, None, None, None, None, None, None])
        self.expected_frame = frames.ContentHeaderFrame(1, expected_payload)

    def when_the_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_deserialise_it_to_a_ContentHeaderFrame(self):
        self.dispatcher.dispatch.assert_called_once_with(self.expected_frame)


class WhenBasicGetOKArrives(MockDispatcherContext):
    def given_a_frame(self):
        self.raw = (
            b'\x01\x00\x01\x00\x00\x00\x22'  # type, channel, size
            b'\x00\x3C\x00\x47'  # 60, 71
            b'\x00\x00\x00\x00\x00\x00\x00\x01'  # delivery tag
            b'\x00'  # not redelivered
            b'\x08exchange'
            b'\x07routing'
            b'\x00\x00\x00\x00'  # no more messages
            b'\xCE')

        expected_method = spec.BasicGetOK(1, False, 'exchange', 'routing', 0)
        self.expected_frame = frames.MethodFrame(1, expected_method)

    def when_the_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_deserialise_it_to_the_correct_method(self):
        self.dispatcher.dispatch.assert_called_once_with(self.expected_frame)
