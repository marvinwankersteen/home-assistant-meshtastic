"""Share HA fixtures with the telemetry regression tests."""

from .test_entity_registration import entry  # noqa: F401 - shared pytest fixture
from .test_ha_compatibility import entries  # noqa: F401 - shared pytest fixture
