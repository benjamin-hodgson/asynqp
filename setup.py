try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()

    from setuptools import setup, find_packages


setup(
    name='asynqp',
    version='0.5.1',
    author="Benjamin Hodgson",
    author_email="benjamin.hodgson@huddle.net",
    url="https://github.com/benjamin-hodgson/asynqp",
    description="An AMQP (aka RabbitMQ) client library for asyncio.",
    package_dir={'': 'src'},
    packages=find_packages('src'),
    package_data={'asynqp': ['amqp0-9-1.xml']},
    install_requires=["setuptools"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Telecommunications Industry",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Networking"
    ]
)
