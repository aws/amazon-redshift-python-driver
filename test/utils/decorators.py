import pytest


def is_numpy_installed() -> bool:
    try:
        import numpy  # type: ignore

        return True
    except ModuleNotFoundError:
        return False


def is_pandas_installed() -> bool:
    try:
        import pandas  # type: ignore

        return True
    except ModuleNotFoundError:
        return False


numpy_only = pytest.mark.skipif(not is_numpy_installed(), reason="requires numpy")

pandas_only = pytest.mark.skipif(not is_pandas_installed(), reason="requires pandas")
