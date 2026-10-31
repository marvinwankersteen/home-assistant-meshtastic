"""Keep existing telemetry entities attached while waiting for gateway data."""

import asyncio
import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.const import STATE_UNAVAILABLE
from homeassistant.helpers import entity_platform
from homeassistant.helpers.entity_platform import EntityPlatform

from custom_components import meshtastic
from custom_components.meshtastic import helpers
from custom_components.meshtastic.aiomeshtastic.protobuf import mesh_pb2, portnums_pb2, telemetry_pb2
from custom_components.meshtastic.api import (
    ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID,
    ATTR_EVENT_MESHTASTIC_API_DATA,
    ATTR_EVENT_MESHTASTIC_API_NODE,
    ATTR_EVENT_MESHTASTIC_API_TELEMETRY_TYPE,
    EVENT_MESHTASTIC_API_TELEMETRY,
    EventMeshtasticApiTelemetryType,
    MeshtasticApiClient,
)
from custom_components.meshtastic.const import (
    CONF_CONNECTION_TCP_HOST,
    CONF_CONNECTION_TCP_PORT,
    CONF_CONNECTION_TYPE,
    CONF_OPTION_FILTER_NODES,
    DOMAIN,
    ConnectionType,
)
from custom_components.meshtastic.sensor import _build_sensors

STAT_FIELDS = {
    "stats_packets_tx": "numPacketsTx",
    "stats_packets_rx": "numPacketsRx",
    "stats_packets_rx_bad": "numPacketsRxBad",
    "stats_packets_rx_duplicate": "numRxDupe",
    "stats_packets_tx_relayed": "numTxRelay",
    "stats_packets_tx_relay_cancelled": "numTxRelayCanceled",
    "stats_nodes_online": "numOnlineNodes",
    "stats_nodes_total": "numTotalNodes",
}
STATS = dict(zip(STAT_FIELDS.values(), [10, 20, 0, 0, 3, 0, 2, 4], strict=True))


@asynccontextmanager
async def sensor_platform(hass, entry):
    platform = EntityPlatform(
        hass=hass,
        logger=logging.getLogger(__name__),
        domain="sensor",
        platform_name=DOMAIN,
        platform=None,
        scan_interval=timedelta(seconds=30),
        entity_namespace=None,
    )
    platform.config_entry = entry

    def add_entities(entities):
        hass.async_create_task(platform.async_add_entities(list(entities)))

    with patch.object(entity_platform, "async_get_current_platform", return_value=platform):
        await helpers.setup_platform_entry(hass, entry, add_entities, _build_sensors)
        await hass.async_block_till_done()
        try:
            yield platform
        finally:
            await helpers.async_unload_entry(hass, entry)
            await platform.async_reset()


def statistics_entities(platform):
    return {
        entity.entity_description.key: entity
        for entity in platform.entities.values()
        if entity.entity_description.key in STAT_FIELDS
    }


async def emit_telemetry(hass, entry, metrics, telemetry_type=EventMeshtasticApiTelemetryType.LOCAL_STATS):
    hass.bus.async_fire(
        EVENT_MESHTASTIC_API_TELEMETRY,
        {
            ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID: entry.entry_id,
            ATTR_EVENT_MESHTASTIC_API_NODE: entry.runtime_data.gateway_node["num"],
            ATTR_EVENT_MESHTASTIC_API_DATA: metrics,
            ATTR_EVENT_MESHTASTIC_API_TELEMETRY_TYPE: telemetry_type,
        },
    )
    await hass.async_block_till_done()


async def test_previously_registered_stats_exist_before_first_packet(hass, entry, entity_registry, device_registry):
    node_id = entry.runtime_data.gateway_node["num"]
    entry.runtime_data.coordinator.data[node_id].pop("localStats")
    existing_ids = {
        key: entity_registry.async_get_or_create(
            "sensor",
            DOMAIN,
            f"{entry.entry_id}_sensor_{node_id}_{key}",
            config_entry=entry,
            suggested_object_id=f"custom_{key}",
        ).entity_id
        for key in STAT_FIELDS
    }
    async with sensor_platform(hass, entry) as platform:
        entities = statistics_entities(platform)
        assert entities.keys() == STAT_FIELDS.keys()
        for key, entity in entities.items():
            assert entity.entity_id == existing_ids[key]
            assert entity.native_value is None
            assert not entity.available
            state = hass.states.get(entity.entity_id)
            assert state.state == STATE_UNAVAILABLE
            assert not state.attributes.get("restored", False)
        await emit_telemetry(hass, entry, STATS)
        for key, entity in entities.items():
            assert entity.available
            assert entity.native_value == STATS[STAT_FIELDS[key]]
            assert hass.states.get(entity.entity_id).state == str(STATS[STAT_FIELDS[key]])


async def test_initial_stats_have_values_without_an_additional_update(hass, entry, entity_registry, device_registry):
    entry.runtime_data.coordinator.data[entry.runtime_data.gateway_node["num"]]["localStats"] = STATS.copy()
    async with sensor_platform(hass, entry) as platform:
        entities = statistics_entities(platform)
        assert len(entities) == len(STAT_FIELDS)
        for key, entity in entities.items():
            assert entity.available
            assert entity.native_value == STATS[STAT_FIELDS[key]]


async def test_first_environment_packet_has_a_value_immediately(hass, entry, entity_registry, device_registry):
    async with sensor_platform(hass, entry) as platform:
        await emit_telemetry(hass, entry, {"temperature": 21.5}, EventMeshtasticApiTelemetryType.ENVIRONMENT_METRICS)
        temperature = next(
            entity
            for entity in platform.entities.values()
            if entity.entity_description.key == "environment_temperature"
        )
        assert temperature.available
        assert temperature.native_value == 21.5
        assert hass.states.get(temperature.entity_id).state == "21.5"


async def test_missing_node_does_not_break_entity_updates(hass, entry, entity_registry, device_registry, caplog):
    async with sensor_platform(hass, entry) as platform:
        entry.runtime_data.coordinator.async_set_updated_data({})
        await hass.async_block_till_done()
        assert all(not entity.available for entity in platform.entities.values())
        assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


@pytest.mark.parametrize("packets_rx", [0, 5])
async def test_protobuf_zero_counters_survive_live_updates_and_database(
    hass, entry, entity_registry, device_registry, packets_rx
):
    node_id = entry.runtime_data.gateway_node["num"]
    node = entry.runtime_data.coordinator.data[node_id]
    node["num"] = node_id
    node["user"]["id"] = f"!{node_id:08x}"
    node.pop("localStats")
    client = MeshtasticApiClient(
        {
            CONF_CONNECTION_TYPE: ConnectionType.TCP.value,
            CONF_CONNECTION_TCP_HOST: "127.0.0.1",
            CONF_CONNECTION_TCP_PORT: 4403,
        },
        hass=hass,
        config_entry_id=entry.entry_id,
    )
    interface = client._interface
    interface._node_database = deepcopy(entry.runtime_data.coordinator.data)
    telemetry = telemetry_pb2.Telemetry()
    telemetry.local_stats.CopyFrom(telemetry_pb2.LocalStats(num_packets_rx=packets_rx))
    packet = mesh_pb2.FromRadio()
    setattr(packet.packet, "from", node_id)
    packet.packet.decoded.portnum = portnums_pb2.PortNum.TELEMETRY_APP
    packet.packet.decoded.payload = telemetry.SerializeToString()
    async with sensor_platform(hass, entry) as platform:
        await interface._process_packet_for_app_listener(packet)
        await asyncio.gather(*tuple(interface._background_tasks))
        await hass.async_block_till_done()
        entities = statistics_entities(platform)
        assert entities.keys() == STAT_FIELDS.keys()
        for key, entity in entities.items():
            expected = packets_rx if key == "stats_packets_rx" else 0
            assert entity.available
            assert entity.native_value == expected
            assert interface.nodes()[node_id]["localStats"][STAT_FIELDS[key]] == expected


@pytest.mark.parametrize("entries", [1], indirect=True)
async def test_options_reload_keeps_gateway_stats_attached(hass, entries, enable_custom_integrations, entity_registry):
    entry = entries[0]
    client = entry.runtime_data.client
    client.connect = AsyncMock()
    gateway_id = entry.runtime_data.gateway_node["num"]
    entry.options[CONF_OPTION_FILTER_NODES].append({"id": gateway_id})
    client_nodes = await client.async_get_all_nodes()
    client_nodes[gateway_id]["localStats"] = STATS.copy()
    with (
        patch.object(meshtastic, "MeshtasticApiClient", return_value=client),
        patch.object(meshtastic, "fetch_meshtastic_hardware_names", AsyncMock(return_value={})),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        unique_id = f"{entry.entry_id}_sensor_{gateway_id}_stats_nodes_total"
        entity_id = entity_registry.async_get_entity_id("sensor", DOMAIN, unique_id)
        assert hass.states.get(entity_id).state == "4"
        del client_nodes[gateway_id]["localStats"]
        hass.config_entries.async_update_entry(entry, options={**entry.options, "test_reload": True})
        await hass.async_block_till_done()
        assert entity_registry.async_get_entity_id("sensor", DOMAIN, unique_id) == entity_id
        state = hass.states.get(entity_id)
        assert state.state == STATE_UNAVAILABLE
        assert not state.attributes.get("restored", False)
        await emit_telemetry(hass, entry, STATS)
        assert hass.states.get(entity_id).state == "4"
        assert await hass.config_entries.async_unload(entry.entry_id)
