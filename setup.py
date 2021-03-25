from setuptools import setup, find_packages

name = 'varbio'
setup(
    version='3.0.1.dev',
    name=name,
    # Only include {name}/, not e.g. tests/
    packages=find_packages(include=(name, name + '.*')),
)
