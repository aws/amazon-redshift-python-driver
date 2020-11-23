import pytest


def is_numpy_installed() -> bool:
    try:
        import numpy

        return True
    except ModuleNotFoundError:
        return False


def is_pandas_installed() -> bool:
    try:
        import pandas

        return True
    except ModuleNotFoundError:
        return False


numpy_only: pytest.mark = pytest.mark.skipif(not is_numpy_installed(), reason="requires numpy")

pandas_only: pytest.mark = pytest.mark.skipif(not is_pandas_installed(), reason="requires pandas")
