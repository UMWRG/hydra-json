#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='hydra-json',
    version='0.1',
    description='App to import and export hydra networks in JSON format',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[],
    entry_points='''
    [console_scripts]
    hydra-json=hydra_json.cli:start_cli
    ''',
)
