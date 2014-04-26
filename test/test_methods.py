from asynqp import methods


class WhenDeserialisingAConnectionStartMethod:
    def given_a_connection_start_method_I_copied_from_the_rabbitmq_server(self):
        self.raw = b"\x00\t\x00\x00\x01%\x0ccapabilitiesF\x00\x00\x00X\x12publisher_confirmst\x01\x1aexchange_exchange_bindingst\x01\nbasic.nackt\x01\x16consumer_cancel_notifyt\x01\tcopyrightS\x00\x00\x00'Copyright (C) 2007-2013 GoPivotal, Inc.\x0binformationS\x00\x00\x005Licensed under the MPL.  See http://www.rabbitmq.com/\x08platformS\x00\x00\x00\nErlang/OTP\x07productS\x00\x00\x00\x08RabbitMQ\x07versionS\x00\x00\x00\x053.1.5\x00\x00\x00\x0eAMQPLAIN PLAIN\x00\x00\x00\x0Ben_US en_GB"

    def when_I_deserialise_the_method(self):
        self.result = methods.ConnectionStart.deserialise(self.raw)

    def it_should_have_the_correct_major_version(self):
        assert self.result.major_version == 0

    def it_should_have_the_correct_minor_version(self):
        assert self.result.minor_version == 9

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
