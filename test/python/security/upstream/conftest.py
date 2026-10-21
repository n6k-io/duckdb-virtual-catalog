import pytest

from conftest import READ, bridge_source


@pytest.fixture
def exposed(upstream, target):
    bridge_source(upstream, target, {"users": READ})
    return upstream, target
