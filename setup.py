from setuptools import setup, find_packages


setup(
	name = 'asynqp',
	packages = find_packages('src'),
	package_dir = {'': 'src'}
)
