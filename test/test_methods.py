from asynqp import methods


class WhenDeserialisingConnectionStart:
    def given_a_connection_start_method_I_copied_from_the_rabbitmq_server(self):
        self.raw = b"\x00\x0A\x00\x0A\x00\t\x00\x00\x01%\x0ccapabilitiesF\x00\x00\x00X\x12publisher_confirmst\x01\x1aexchange_exchange_bindingst\x01\nbasic.nackt\x01\x16consumer_cancel_notifyt\x01\tcopyrightS\x00\x00\x00'Copyright (C) 2007-2013 GoPivotal, Inc.\x0binformationS\x00\x00\x005Licensed under the MPL.  See http://www.rabbitmq.com/\x08platformS\x00\x00\x00\nErlang/OTP\x07productS\x00\x00\x00\x08RabbitMQ\x07versionS\x00\x00\x00\x053.1.5\x00\x00\x00\x0eAMQPLAIN PLAIN\x00\x00\x00\x0Ben_US en_GB"

    def when_I_deserialise_the_method(self):
        self.result = methods.ConnectionStart.deserialise(self.raw)

    def it_should_have_the_correct_version(self):
        assert self.result.version == (0, 9)

    def it_should_have_the_expected_server_properties(self):
        assert self.result.server_properties == {
            'capabilities': {'publisher_confirms': True,
                             'exchange_exchange_bindings': True,
                             'basic.nack': True,
                             'consumer_cancel_notify': True},
            'copyright': 'Copyright (C) 2007-2013 GoPivotal, Inc.',
            'information': 'Licensed under the MPL.  See http://www.rabbitmq.com/',
            'platform': 'Erlang/OTP',
            'product': 'RabbitMQ',
            'version': '3.1.5'
        }

    def it_should_have_the_security_mechanisms(self):
        assert self.result.mechanisms == {'AMQPLAIN', 'PLAIN'}

    def it_should_have_the_correct_locals(self):
        assert self.result.locales == {'en_US', 'en_GB'}


class WhenSerialisingConnectionStartOK:
    def given_a_method_to_send(self):
        self.method = methods.ConnectionStartOK({'somecrap': 'aboutme'}, 'AMQPLAIN', {'auth':'info'}, 'en_US')

    def when_we_serialise_the_method(self):
        self.result = self.method.serialise()

    def it_should_return_the_correct_bytestring(self):
        assert self.result == b'\x00\x0A\x00\x0B\x00\x00\x00\x15\x08somecrapS\x00\x00\x00\x07aboutme\x08AMQPLAIN\x00\x00\x00\x0E\x04authS\x00\x00\x00\x04info\x05en_US'


class WhenDeserialisingConnectionTune:
    def given_a_connection_tune_method_I_copied_from_the_rabbitmq_server(self):
        self.raw = b'\x00\x0A\x00\x1E\x00\x00\x00\x02\x00\x00\x02\x58'

    def when_I_deserialise_the_method(self):
        self.result = methods.ConnectionTune.deserialise(self.raw)

    def it_should_have_the_correct_max_channel(self):
        assert self.result.max_channel == 0

    def it_should_have_the_correct_max_frame_length(self):
        assert self.result.max_frame_length == 131072

    def it_shoud_have_the_correct_heartbeat(self):
        assert self.result.heartbeat_interval == 600
