import asyncio
import asynqp


class WhenConnectingToRabbit:
    def given_the_loop(self):
        self.loop = asyncio.get_event_loop()

    def when_I_connect(self):
        self.connection = self.loop.run_until_complete(asynqp.connect())

    def it_should_connect(self):
        assert hasattr(self, 'connection')
