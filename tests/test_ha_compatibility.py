"""Exercise HA 2026.10 registry ownership and the serialx transport."""

import asyncio
import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest
import serialx
from homeassistant.const import UnitOfDensity
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components import meshtastic
from custom_components.meshtastic.aiomeshtastic.connection.serial import SerialConnection, SerialConnectionError
from custom_components.meshtastic.api import (
    ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID,
    ATTR_EVENT_MESHTASTIC_API_DATA,
    EVENT_MESHTASTIC_API_TEXT_MESSAGE,
)
from custom_components.meshtastic.const import (
    CONF_OPTION_FILTER_NODES,
    DOMAIN,
    EVENT_MESHTASTIC_DOMAIN_EVENT,
    EVENT_MESHTASTIC_DOMAIN_MESSAGE_LOG,
)
from custom_components.meshtastic.coordinator import MeshtasticDataUpdateCoordinator
from custom_components.meshtastic.data import DATA_COMPONENT, MeshtasticData
from custom_components.meshtastic.device_tracker import MeshtasticDeviceTracker, _build_device_trackers
from custom_components.meshtastic.entity import GatewayChannelEntity, GatewayDirectMessageEntity
from custom_components.meshtastic.logbook import async_setup_message_logger
from custom_components.meshtastic.sensor import _build_air_quality_metrics_sensors

REMOTE = 123456
GATEWAY = 2658611232


def node(node_id):
    return {
        "num": node_id,
        "user": {
            "id": f"!{node_id:08x}",
            "shortName": "📡",
            "longName": f"Node {node_id}",
            "hwModel": "HELTEC_V3",
        },
        "position": {"latitude": 52.0, "longitude": 13.0},
        "deviceMetrics": {"batteryLevel": 75, "voltage": 4.0},
    }


@pytest.fixture
def entries(hass, request):
    result = []
    for index in range(getattr(request, "param", 2)):
        entry = MockConfigEntry(
            domain=DOMAIN,
            version=1,
            minor_version=2,
            unique_id=str(GATEWAY + index),
            options={CONF_OPTION_FILTER_NODES: [{"id": REMOTE}]},
        )
        entry.add_to_hass(hass)
        gateway = node(GATEWAY + index)
        # Deliberately return the remote node first: its gateway must exist first.
        nodes = {REMOTE: node(REMOTE), gateway["num"]: gateway}
        client = Mock(
            metadata={"firmwareVersion": "test"},
            async_get_own_node=AsyncMock(return_value=gateway),
            get_own_node=Mock(return_value=gateway),
            async_get_all_nodes=AsyncMock(return_value=nodes),
            async_get_node_local_config=AsyncMock(return_value={}),
            async_get_node_module_config=AsyncMock(return_value={}),
            async_get_channels=AsyncMock(
                return_value=[
                    {
                        "index": 0,
                        "role": "PRIMARY",
                        "settings": {"name": "Test", "psk": "AQ==", "uplinkEnabled": True, "downlinkEnabled": True},
                    }
                ]
            ),
            get_node_info=Mock(return_value=None),
            disconnect=AsyncMock(),
        )
        coordinator = MeshtasticDataUpdateCoordinator(hass)
        coordinator.config_entry = entry
        coordinator.data = deepcopy(nodes)
        coordinator.last_update_success = True
        entry.runtime_data = MeshtasticData(
            client=client, coordinator=coordinator, integration=Mock(), gateway_node=gateway
        )
        result.append(entry)
    return result


async def setup_devices(hass, entries):
    with patch.object(meshtastic, "fetch_meshtastic_hardware_names", AsyncMock(return_value={})):
        for entry in entries:
            await meshtastic._setup_meshtastic_devices(hass, entry, entry.runtime_data.client)


async def test_devices_are_owned_by_the_correct_entry(hass, entries, device_registry, caplog):
    await setup_devices(hass, entries)
    remote_devices = []
    for entry in entries:
        remote = device_registry.async_get_device_by_identifier((DOMAIN, str(REMOTE)), entry.entry_id)
        gateway = device_registry.async_get_device_by_identifier(
            (DOMAIN, str(entry.runtime_data.gateway_node["num"])), entry.entry_id
        )
        assert remote.config_entry_id == entry.entry_id
        assert remote.via_device_id == gateway.id
        assert gateway.via_device_id is None
        remote_devices.append(remote)
    assert remote_devices[0].id != remote_devices[1].id
    await setup_devices(hass, entries)
    assert (
        device_registry.async_get_device_by_identifier((DOMAIN, str(REMOTE)), entries[0].entry_id).id
        == remote_devices[0].id
    )
    await meshtastic._remove_meshtastic_device(device_registry, entries[0], REMOTE)
    assert device_registry.async_get_device_by_identifier((DOMAIN, str(REMOTE)), entries[0].entry_id) is None
    assert device_registry.async_get(remote_devices[1].id) is not None
    await meshtastic._remove_meshtastic_device(device_registry, entries[0], REMOTE)
    assert "deprecated" not in caplog.text


async def test_gateway_entities_use_config_entry_platform(
    hass, entries, device_registry, entity_registry, enable_custom_integrations, caplog
):
    await setup_devices(hass, entries)
    assert await meshtastic.async_setup(hass, {})
    hass.config.components.add(DOMAIN)
    component = hass.data[DATA_COMPONENT]
    previous_ids = []
    for entry in entries:
        unique_id = GatewayChannelEntity.build_unique_id(entry.entry_id, entry.runtime_data.gateway_node["num"], 0)
        previous = entity_registry.async_get_or_create(
            DOMAIN, DOMAIN, unique_id, config_entry=entry, suggested_object_id=f"my_channel_{entry.entry_id.lower()}"
        )
        previous_ids.append(previous.entity_id)
        assert await component.async_setup_entry(entry)
    await hass.async_block_till_done()
    assert len(list(component.entities)) == 6
    for entity in component.entities:
        entry_id = entity.registry_entry.config_entry_id
        assert entry_id in {entry.entry_id for entry in entries}
        device = device_registry.async_get(entity.registry_entry.device_id)
        assert device.config_entry_id == entry_id
        assert entity.platform.config_entry.entry_id == entry_id
    assert all(component.get_entity(entity_id) is not None for entity_id in previous_ids)
    assert "without a config entry" not in caplog.text
    assert "deprecated" not in caplog.text
    assert await component.async_unload_entry(entries[0])
    assert len(list(component.entities)) == 3
    assert await component.async_setup_entry(entries[0])
    assert len(list(component.entities)) == 6
    for entry in entries:
        assert await component.async_unload_entry(entry)
    assert not list(component.entities)


@pytest.mark.parametrize("target", ["channel", "node"])
async def test_message_events_use_matching_config_entry(
    hass, entries, device_registry, entity_registry, target, caplog
):
    await setup_devices(hass, entries)
    events, messages, listeners = [], [], []
    hass.bus.async_listen(EVENT_MESHTASTIC_DOMAIN_EVENT, events.append)
    hass.bus.async_listen(EVENT_MESHTASTIC_DOMAIN_MESSAGE_LOG, messages.append)
    for entry in entries:
        listeners.append(await async_setup_message_logger(hass, entry))
        gateway_id = entry.runtime_data.gateway_node["num"]
        uid = (
            GatewayChannelEntity.build_unique_id(entry.entry_id, gateway_id, 0)
            if target == "channel"
            else GatewayDirectMessageEntity.build_unique_id(entry.entry_id, gateway_id)
        )
        entity_registry.async_get_or_create(DOMAIN, DOMAIN, uid, config_entry=entry)
    for entry in entries:
        gateway_id = entry.runtime_data.gateway_node["num"]
        events.clear()
        messages.clear()
        hass.bus.async_fire(
            EVENT_MESHTASTIC_API_TEXT_MESSAGE,
            {
                ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID: entry.entry_id,
                ATTR_EVENT_MESHTASTIC_API_DATA: {
                    "from": REMOTE,
                    "gateway": gateway_id,
                    "to": {target: 0 if target == "channel" else gateway_id},
                    "message": "hello",
                },
            },
        )
        await hass.async_block_till_done()
        assert len(events) == 2
        assert len(messages) == 1
        assert all(
            device_registry.async_get(event.data["device_id"]).config_entry_id == entry.entry_id for event in events
        )
    for remove in listeners:
        remove()
    assert "deprecated" not in caplog.text


@pytest.mark.parametrize(("level", "expected"), [(75, 75), (101, 100), (-1, 0), (None, None)])
def test_tracker_battery_is_an_extra_attribute(entries, level, expected):
    runtime = entries[0].runtime_data
    runtime.coordinator.data[REMOTE]["deviceMetrics"]["batteryLevel"] = level
    tracker = next(
        entity for entity in _build_device_trackers(runtime.coordinator.data, runtime) if entity.node_id == REMOTE
    )
    tracker._async_update_attrs()
    assert "battery_level" not in MeshtasticDeviceTracker.__dict__
    assert tracker.extra_state_attributes.get("battery_level") == expected
    del runtime.coordinator.data[REMOTE]["deviceMetrics"]["batteryLevel"]
    tracker._async_update_attrs()
    assert "battery_level" not in tracker.extra_state_attributes


def test_air_quality_units_keep_values_and_unique_ids(entries):
    runtime = entries[0].runtime_data
    runtime.coordinator.data[REMOTE]["airQualityMetrics"] = {"pm25Standard": 12.5}
    sensors = list(_build_air_quality_metrics_sensors(runtime.coordinator.data, runtime))
    assert len(sensors) == 1
    sensor = sensors[0]
    sensor._async_update_attrs()
    assert sensor.native_unit_of_measurement == UnitOfDensity.MICROGRAMS_PER_CUBIC_METER
    assert sensor.native_value == 12.5
    assert sensor.unique_id == f"{entries[0].entry_id}_sensor_{REMOTE}_airquality_pm25_standard"


def test_imports_do_not_report_retired_api_usage():
    result = subprocess.run(  # noqa: S603 - fixed interpreter and import-only command
        [
            sys.executable,
            "-c",
            "import custom_components.meshtastic.sensor; import custom_components.meshtastic.device_tracker",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "deprecated" not in result.stderr
    manifest = json.loads(Path("custom_components/meshtastic/manifest.json").read_text())
    assert "serialx" in manifest["requirements"]
    assert not any("pyserial" in requirement for requirement in manifest["requirements"])


async def test_serialx_real_virtual_port_roundtrip():
    master, slave = os.openpty()
    connection = SerialConnection(os.ttyname(slave), debug_logs=True)
    try:
        await connection._connect()
        assert connection.is_connected
        assert await connection._write_bytes(b"outbound")
        received = await asyncio.wait_for(asyncio.to_thread(os.read, master, 8), 2)
        assert received == b"outbound"
        os.write(master, b"inbound")
        assert await asyncio.wait_for(connection._read_bytes(exactly=7), 2) == b"inbound"
        await connection._on_other_data(b"\x1b[32mtest log\n")
        await connection._disconnect()
        assert not connection.is_connected
        assert not await connection._write_bytes(b"after close")
        assert connection._log_reader_task is None
    finally:
        await connection._disconnect()
        os.close(master)
        os.close(slave)


@pytest.mark.parametrize("error", [OSError("read failed"), TimeoutError(), serialx.SerialException("serial failure")])
async def test_serialx_read_errors_are_wrapped(error):
    connection = SerialConnection("/dev/test")
    connection._reader = Mock(at_eof=Mock(return_value=False), read=AsyncMock(side_effect=error))
    with pytest.raises(SerialConnectionError) as exception:
        await connection._read_bytes()
    assert exception.value.__cause__ is error


@pytest.mark.parametrize("entries", [1, 2], indirect=True)
async def test_full_integration_setup_and_unload(hass, entries, enable_custom_integrations, caplog):
    """Load all platforms through HA with only the radio client mocked."""
    entry = entries[0]
    clients = {entry.entry_id: entry.runtime_data.client for entry in entries}
    for client in clients.values():
        client.connect = AsyncMock()
    with (
        patch.object(
            meshtastic, "MeshtasticApiClient", side_effect=lambda data, hass, config_entry_id: clients[config_entry_id]
        ),
        patch.object(meshtastic, "fetch_meshtastic_hardware_names", AsyncMock(return_value={})),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        assert len(list(hass.data[DATA_COMPONENT].entities)) == 3 * len(entries)
        assert "deprecated" not in caplog.text
        assert "without a config entry" not in caplog.text
        assert "does not generate unique IDs" not in caplog.text
        for entry in entries:
            assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
        assert not list(hass.data[DATA_COMPONENT].entities)
