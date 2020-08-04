#!/usr/bin/env python

from setuptools import find_packages, setup

long_description = """\
redshift_connector is a Pure-Python interface to the Amazon Redshift. """

version = "2.0.0.0"

setup(
    name="redshift_connector",
    version=version,
    description="Redshift interface library",
    long_description=long_description,
    license="BSD",
    python_requires='>=3.5',
    install_requires=[
        'scramp==1.1.1',
        'pytest==5.4.3',
        'pytz==2020.1',
        'bs4==0.0.1',
        'boto3==1.14.5',
        'requests==2.23.0',
        'lxml==4.5.1',
        'botocore==1.17.5',
        'mypy==0.782',
        'pre-commit==2.6.0',
        'numpy==1.19.0',
        'pandas==1.0.5',
        'pytest-cov==2.10.0'
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: Jython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="redshift dbapi",
    include_package_data=True,
    packages=find_packages()
)
