"""Regression tests for duplicate registration and invalid entity IDs."""

import logging
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from homeassistant.core import valid_entity_id
from homeassistant.helpers import entity_platform
from homeassistant.helpers.entity_platform import EntityPlatform
from homeassistant.helpers.entity_registry import RegistryEntryDisabler
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.meshtastic import helpers
from custom_components.meshtastic.binary_sensor import _build_binary_sensors
from custom_components.meshtastic.const import CONF_OPTION_FILTER_NODES
from custom_components.meshtastic.coordinator import MeshtasticDataUpdateCoordinator
from custom_components.meshtastic.data import MeshtasticData
from custom_components.meshtastic.device_tracker import _build_device_trackers
from custom_components.meshtastic.sensor import _build_sensors

NODE_ID = 2658611232
FACTORIES = [
    ("sensor", _build_sensors),
    ("binary_sensor", _build_binary_sensors),
    ("device_tracker", _build_device_trackers),
]


@pytest.fixture
def entry(hass):
    config_entry = MockConfigEntry(
        domain="meshtastic",
        entry_id="AAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        options={CONF_OPTION_FILTER_NODES: [{"id": NODE_ID}]},
    )
    config_entry.add_to_hass(hass)
    coordinator = MeshtasticDataUpdateCoordinator(hass)
    coordinator.config_entry = config_entry
    coordinator.data = {
        NODE_ID: {
            "num": NODE_ID,
            "user": {"shortName": "📡", "longName": "Test gateway"},
            "position": {"latitude": 52.0, "longitude": 13.0},
            "deviceMetrics": {"batteryLevel": 75, "voltage": 4.0},
            "localStats": {"numOnlineNodes": 2, "numTotalNodes": 3},
        }
    }
    coordinator.last_update_success = True
    gateway = coordinator.data[NODE_ID]
    config_entry.runtime_data = MeshtasticData(
        client=Mock(get_own_node=Mock(return_value=gateway)),
        coordinator=coordinator,
        integration=Mock(),
        gateway_node=gateway,
    )
    return config_entry


@pytest.mark.parametrize(("domain", "factory"), FACTORIES)
@pytest.mark.parametrize("short_name", ["📡", "A.B / Test!", "ÄÖ Üß", "", None, "GW01"])
def test_valid_id_suggestions_and_unchanged_unique_ids(entry, domain, factory, short_name):
    """Names cannot produce an invalid ID or change entity identity."""
    gateway = entry.runtime_data.gateway_node
    gateway["user"]["shortName"] = short_name
    for entity in factory(helpers.get_nodes(entry), entry.runtime_data):
        assert entity.entity_id is None
        assert valid_entity_id(f"{domain}.{entity.suggested_object_id}")
        assert entity.unique_id == f"{entry.entry_id}_{domain}_{NODE_ID}_{entity.entity_description.key}"


@pytest.mark.parametrize(("domain", "factory"), FACTORIES)
@pytest.mark.parametrize("registration_state", ["pending", "renamed", "disabled"])
async def test_updates_do_not_register_duplicates(hass, entry, domain, factory, registration_state):
    """Repeated updates must not re-add pending, renamed or disabled entities."""
    platform = SimpleNamespace(domain=domain, entities={})
    batches = []

    def add_entities(entities):
        batch = list(entities)
        batches.append(batch)
        if registration_state == "renamed":
            for index, entity in enumerate(batch):
                entity.entity_id = f"{domain}.custom_{index}"
                platform.entities[entity.entity_id] = entity

    with patch.object(entity_platform, "async_get_current_platform", return_value=platform):
        await helpers.setup_platform_entry(hass, entry, add_entities, factory)
        initial_ids = [entity.unique_id for entity in batches[0]]
        assert initial_ids
        for _ in range(5):
            entry.runtime_data.coordinator.async_update_listeners()
        assert len(batches) == 1

        # New metrics or a new node must still be discovered exactly once.
        new_node_id = NODE_ID + 1
        entry.options[CONF_OPTION_FILTER_NODES].append({"id": new_node_id})
        entry.runtime_data.coordinator.data[new_node_id] = deepcopy(entry.runtime_data.gateway_node)
        entry.runtime_data.coordinator.async_update_listeners()
        entry.runtime_data.coordinator.async_update_listeners()
        assert len(batches) == 2
        all_ids = [entity.unique_id for batch in batches for entity in batch]
        assert len(all_ids) == len(set(all_ids))
        assert await helpers.async_unload_entry(hass, entry)
        entry.runtime_data.coordinator.async_update_listeners()
        assert len(batches) == 2

        # Reload starts a fresh registration cycle and removes old listeners.
        await helpers.setup_platform_entry(hass, entry, add_entities, factory)
        entry.runtime_data.coordinator.async_update_listeners()
        assert len(batches) == 3
        assert await helpers.async_unload_entry(hass, entry)


@pytest.mark.parametrize(("domain", "factory"), FACTORIES)
async def test_late_node_data_is_discovered_once(hass, entry, domain, factory):
    """An initially empty node list must not prevent later registration."""
    platform = SimpleNamespace(domain=domain, entities={})
    add_entities = Mock()
    nodes = entry.runtime_data.coordinator.data
    entry.runtime_data.coordinator.data = {}
    with patch.object(entity_platform, "async_get_current_platform", return_value=platform):
        await helpers.setup_platform_entry(hass, entry, add_entities, factory)
        add_entities.assert_not_called()
        entry.runtime_data.coordinator.data = nodes
        entry.runtime_data.coordinator.async_update_listeners()
        entry.runtime_data.coordinator.async_update_listeners()
        add_entities.assert_called_once()
        assert await helpers.async_unload_entry(hass, entry)


@pytest.mark.parametrize(("domain", "factory"), FACTORIES)
@pytest.mark.parametrize("disabled", [False, True])
async def test_real_ha_registry_preserves_existing_ids(
    hass, entry, domain, factory, disabled, entity_registry, device_registry, caplog
):
    """Use HA's real platform and registry, including existing custom IDs."""
    entities = list(factory(helpers.get_nodes(entry), entry.runtime_data))
    existing = entity_registry.async_get_or_create(
        domain,
        "meshtastic",
        entities[0].unique_id,
        config_entry=entry,
        suggested_object_id="existing_custom_name",
        disabled_by=RegistryEntryDisabler.USER if disabled else None,
    )
    platform = EntityPlatform(
        hass=hass,
        logger=logging.getLogger(__name__),
        domain=domain,
        platform_name="meshtastic",
        platform=None,
        scan_interval=timedelta(seconds=30),
        entity_namespace=None,
    )
    platform.config_entry = entry
    batches = []

    def add_entities(new_entities):
        batch = list(new_entities)
        batches.append(batch)
        hass.async_create_task(platform.async_add_entities(batch))

    with patch.object(entity_platform, "async_get_current_platform", return_value=platform):
        await helpers.setup_platform_entry(hass, entry, add_entities, factory)
        # Trigger an update before HA has finished processing the batch.
        entry.runtime_data.coordinator.async_update_listeners()
        await hass.async_block_till_done()
        assert len(platform.entities) == len(entities) - int(disabled)
        assert (existing.entity_id in platform.entities) is not disabled
        assert entity_registry.async_get(existing.entity_id).disabled is disabled
        assert all(valid_entity_id(entity_id) for entity_id in platform.entities)
        for _ in range(3):
            entry.runtime_data.coordinator.async_update_listeners()
        await hass.async_block_till_done()
        assert len(batches) == 1
        assert "does not generate unique IDs" not in caplog.text
        assert "invalid entity ID" not in caplog.text
        assert await helpers.async_unload_entry(hass, entry)
    await platform.async_reset()
