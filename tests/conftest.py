import pytest


@pytest.fixture(scope="session")
def spark():
    from src.spark import get_spark

    session = get_spark()
    yield session
    session.stop()
