class ConnectionBuilder:
    def build(self):
        self.connection = asynqp.Connection('guest', 'guest', loop=self.loop)
        self.connection.protocol = self.protocol = mock.Mock(spec=asynqp.AMQP)
        self.protocol.transport = mock.Mock()
