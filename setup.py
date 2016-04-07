
from setuptools import setup, find_packages

setup(
    name="HelloWorld",
    version="0.1",
    packages=find_packages(),
    description='Python Distribution Utilities',
    author='Chris Wilkes',
    author_email='cwilkes@gmail.com',
    url='http://ladro.com',
    install_requires=['requests>=2.3.0', 'boto3>=1.3.0'],
    scripts=['cloud_runner.py', ],
    entry_points={
        'console_scripts': [
            'ccm = cloud_runner:main_func',
        ],
    }
)
