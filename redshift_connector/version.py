# We use the pyproject.toml to store the version
# and read it at runtime using importlib_metadata for python < 3.7
from importlib_metadata import version

__version__ = version(__name__.split(".")[0])