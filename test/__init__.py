import asyncio
import logging
from .util import testing_exception_handler


loop = asyncio.get_event_loop()
loop.set_exception_handler(testing_exception_handler)


logging.getLogger('asynqp').setLevel(100)  # mute the logger
