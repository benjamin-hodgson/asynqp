import asyncio
import asynqp
from asynqp import methods
from .base_contexts import ProtocolContext


class WhenConnectionStartArrives(ProtocolContext):
    def given_a_connection_start_method_I_copied_from_the_rabbitmq_server(self):
        self.raw = b"\x01\x00\x00\x00\x00\x01\x50\x00\x0A\x00\x0A\x00\t\x00\x00\x01%\x0ccapabilitiesF\x00\x00\x00X\x12publisher_confirmst\x01\x1aexchange_exchange_bindingst\x01\nbasic.nackt\x01\x16consumer_cancel_notifyt\x01\tcopyrightS\x00\x00\x00'Copyright (C) 2007-2013 GoPivotal, Inc.\x0binformationS\x00\x00\x005Licensed under the MPL.  See http://www.rabbitmq.com/\x08platformS\x00\x00\x00\nErlang/OTP\x07productS\x00\x00\x00\x08RabbitMQ\x07versionS\x00\x00\x00\x053.1.5\x00\x00\x00\x0eAMQPLAIN PLAIN\x00\x00\x00\x0Ben_US en_GB\xCE"

        expected_method = methods.ConnectionStart(0, 9, {
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
        self.expected_frame = asynqp.Frame(asynqp.FrameType.method, 0, expected_method)

    def when_the_frame_arrives(self):
        self.protocol.data_received(self.raw)

    def it_should_dispatch_a_correctly_deserialised_ConnectionStart_method(self):
        self.connection.handle.assert_called_once_with(self.expected_frame)


class WhenSendingConnectionStartOK(ProtocolContext):
    def given_a_method_to_send(self):
        method = methods.ConnectionStartOK({'somecrap': 'aboutme'}, 'AMQPLAIN', {'auth': 'info'}, 'en_US')
        self.frame = asynqp.Frame(asynqp.FrameType.method, 0, method)

    def when_we_send_the_method(self):
        self.protocol.send_frame(self.frame)

    def it_should_send_the_correct_bytestring(self):
        self.transport.write.assert_called_once_with(b'\x01\x00\x00\x00\x00\x00>\x00\n\x00\x0b\x00\x00\x00\x15\x08somecrapS\x00\x00\x00\x07aboutme\x08AMQPLAIN\x00\x00\x00\x0e\x04authS\x00\x00\x00\x04info\x05en_US\xce')


class WhenDeserialisingConnectionTune(ProtocolContext):
    def given_a_connection_tune_method_I_copied_from_the_rabbitmq_server(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x0C\x00\x0A\x00\x1E\x00\x00\x00\x02\x00\x00\x02\x58\xCE'
        expected_method = methods.ConnectionTune(0, 131072, 600)
        self.expected_frame = asynqp.Frame(asynqp.FrameType.method, 0, expected_method)

    def when_the_frame_arrives(self):
        self.protocol.data_received(self.raw)

    def it_should_dispatch_a_correctly_deserialised_ConnectionTune_method(self):
        self.connection.handle.assert_called_once_with(self.expected_frame)


class WhenSendingConnectionTuneOK(ProtocolContext):
    def given_a_method_to_send(self):
        method = methods.ConnectionTuneOK(1024, 131072, 10)
        self.frame = asynqp.Frame(asynqp.FrameType.method, 0, method)

    def when_I_serialise_the_method(self):
        self.protocol.send_frame(self.frame)

    def it_should_write_the_correct_bytestring(self):
        self.transport.write.assert_called_once_with(b'\x01\x00\x00\x00\x00\x00\x0C\x00\n\x00\x1F\x04\x00\x00\x02\x00\x00\x00\x0A\xCE')
