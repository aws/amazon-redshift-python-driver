#!/usr/bin/env python
import os
import sys

from setuptools import Command, find_packages, setup
from setuptools.command.install import install as InstallCommandBase
from setuptools.command.test import test as TestCommand
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel as BDistWheelCommandBase


class NoopCommand(Command):
    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        pass


class BasePytestCommand(TestCommand):
    user_options = []
    test_dir = None

    def initialize_options(self):
        TestCommand.initialize_options(self)

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest

        src_dir = os.getenv("SRC_DIR", "")
        if src_dir:
            src_dir += "/"
        args = [self.test_dir, "--cov=redshift_connector", "--cov-report=xml", "--cov-report=html"]
        print(args)

        errno = pytest.main(args)
        sys.exit(errno)


class UnitTestCommand(BasePytestCommand):
    test_dir = "test/unit"


class IntegrationTestCommand(BasePytestCommand):
    test_dir = "test/integration"


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


class InstallCommand(InstallCommandBase):
    """Override the installation dir."""

    def finalize_options(self):
        ret = InstallCommandBase.finalize_options(self)
        self.install_lib = self.install_platlib
        return ret


class BDistWheelCommand(BDistWheelCommandBase):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False
        self.universal = True

    def get_tag(self):
        python, abi, plat = "py3", "none", "any"
        return python, abi, plat


# read the contents of your README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()
exec(open("redshift_connector/version.py").read())

setup(
    name="redshift_connector",
    version=__version__,
    description="Redshift interface library",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    author="Amazon Web Services",
    author_email="redshift-drivers@amazon.com",
    url="https://github.com/aws/amazon-redshift-python-driver",
    license="Apache License 2.0",
    python_requires=">=3.5",
    install_requires=[
        "scramp>=1.2.0<1.3.0",
        "pytz>=2020.1<2020.2",
        "BeautifulSoup4>=4.7.0<4.8.0",
        "boto3>=1.16.8<1.17.0",
        "requests>=2.23.0<2.24.0",
        "lxml>=4.2.5<4.6.0",
        "botocore>=1.19.8<1.20.0",
        "numpy>=1.15.4<1.20.0",
        "pandas==0.25.3",
        "wheel>=0.33",
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
    package_data={"redshift-connector": ["*.py", "*.crt", "LICENSE", "NOTICE"]},
    packages=find_packages(exclude=["test*"]),
    cmdclass={
        "install": InstallCommand,
        "bdist_wheel": BDistWheelCommand,
        "unit_test": UnitTestCommand,
        "integration_test": IntegrationTestCommand,
    },
)
