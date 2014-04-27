from ez_setup import use_setuptools
use_setuptools()


from setuptools import setup, find_packages


setup(
    name = 'asynqp',
    version = '0.1',
    author = "Benjamin Hodgson",
    author_email = "benjamin.hodgson@huddle.net",
    description = "An AMQP (aka RabbitMQ) client library for asyncio.",
    url = "https://github.com/benjamin-hodgson/asynqp",
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    package_data = {'asynqp': ['amqp0-9-1.xml']},
    install_requires = ["setuptools"]
)
